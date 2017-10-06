package brokerage_server_ib

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/inconshreveable/log15"

	"github.com/dimfeld/brokerage_server/types"
	"github.com/dimfeld/ib"
)

type replyBehavior int

type activeReply struct {
	dataChan chan ib.Reply
	ctx      context.Context
}

var (
	// This should never happen since we don't burst messages.
	ErrWayTooFast       = errors.New("Could not send message at this rate")
	ErrConnectionClosed = errors.New("broker connection closed")
)

const (
	REPLY_CONTINUE replyBehavior = iota
	REPLY_DONE
)

type callbackFunc func(r ib.Reply) (replyBehavior, error)

func (p *IB) handleMatchedReply(r ib.MatchedReply) {
	id := r.ID()

	p.activeMutex.Lock()
	reply, ok := p.active[id]
	p.activeMutex.Unlock()

	if !ok {
		// Got an unexpected reply
		// This will actually be a fairly normal occurrence so don't warn on it.
		prettyPrint := func() string {
			return fmt.Sprintf("%T:%+v", r, r)
		}
		p.LogDebugVerbose("Unexpected reply", "msg", log15.Lazy{prettyPrint})
		return
	}

	select {
	case reply.dataChan <- r:
	case <-reply.ctx.Done():
	}
}

func (p *IB) subscribeUnmatched() (dataChan chan ib.Reply, closeFunc func()) {
	dataChan = make(chan ib.Reply, 1)
	p.unmChannelMutex.Lock()
	p.unmChannelTag += 1
	chanTag := p.unmChannelTag
	p.unmChannels[p.unmChannelTag] = dataChan
	p.unmChannelMutex.Unlock()

	closeFunc = func() {
		p.unmChannelMutex.Lock()
		c := p.unmChannels[chanTag]
		delete(p.unmChannels, chanTag)
		close(c)
		p.unmChannelMutex.Unlock()
	}

	return
}

func (p *IB) handleReply(rep ib.Reply) {
	prettyPrint := func() string {
		return fmt.Sprintf("%T:%+v", rep, rep)
	}
	p.LogDebugTrace("received", "msg", log15.Lazy{prettyPrint})

	switch r := rep.(type) {
	case *ib.ManagedAccounts:
		p.Accounts = r.AccountsList
		p.startAccountData()

	case *ib.AccountValue:
		p.handleAccountValue(r)

	case *ib.PortfolioValue:
		p.handlePortfolioValue(r)

	case *ib.NextValidID:
		p.LogDebugTrace("NextValidID", "id", r.OrderID)
		atomic.StoreInt64(&p.nextOrderIdValue, r.OrderID)
		p.open = true
		if p.connectChan != nil {
			close(p.connectChan)
			p.connectChan = nil
		}

	case *ib.ErrorMessage:
		id := r.ID()

		// TODO Some errors are not actually replies to the
		// request, and should be handled here instead of passed
		// through.

		if id == -1 {
			if r.SeverityWarning() {
				p.Logger.Warn("info", "err", r)
			} else {
				p.Logger.Error("received error", "err", r)
			}
		} else {
			p.handleMatchedReply(r)
			// Make sure the request gets closed, since nothing else is coming in.
			p.closeMatchedRequest(id)
		}

	case ib.MatchedReply:
		p.handleMatchedReply(r)

	default:
		p.unmChannelMutex.RLock()
		for _, c := range p.unmChannels {
			c <- rep
		}
		p.unmChannelMutex.RUnlock()
	}
}

func (p *IB) nextOrderID() int64 {
	return atomic.AddInt64(&p.nextOrderIdValue, 1)
}

func (p *IB) send(r ib.Request) error {
	res := p.rateLimiter.Reserve()
	if !res.OK() {
		// This should never happen since we don't burst messages.
		return ErrWayTooFast
	}

	if waitTime := res.Delay(); waitTime > 0 {
		p.LogDebugVerbose("Rate limit", "delay", waitTime, "req", r)
		time.Sleep(waitTime)
	}

	prettyPrint := func() string {
		return fmt.Sprintf("%T:%+v", r, r)
	}
	p.LogDebugTrace("Sending", "msg", log15.Lazy{prettyPrint})
	err := p.engine.Send(r)
	if err == io.EOF {
		err = types.ErrDisconnected
	}
	return err
}

func (p *IB) sendMatchedRequest(ctx context.Context, r ib.MatchedRequest) (nextId int64, dataChan chan ib.Reply, err error) {
	nextId = r.ID()
	if nextId == 0 {
		nextId = p.nextOrderID()
		r.SetID(nextId)
	}

	p.activeMutex.Lock()
	rep, ok := p.active[nextId]
	if !ok {
		dataChan = make(chan ib.Reply, 1)
		p.active[nextId] = activeReply{
			dataChan: dataChan,
			ctx:      ctx,
		}
	} else {
		dataChan = rep.dataChan
	}
	p.activeMutex.Unlock()

	err = p.send(r)
	return
}

// The mutex MUST be acquired before calling this function!
func (p *IB) closeMatchedRequestWithoutMutex(id int64) {
	rep, ok := p.active[id]
	if ok {
	outer:
		for {
			// Drain the channel, if it needs it.
			select {
			case <-rep.dataChan:
			default:
				break outer
			}
		}

		delete(p.active, id)
		if rep.dataChan != nil {
			close(rep.dataChan)
		}
	}
}

func (p *IB) closeMatchedRequest(id int64) {
	p.activeMutex.Lock()
	p.closeMatchedRequestWithoutMutex(id)
	p.activeMutex.Unlock()
}

func (p *IB) startStreamingRequest(ctx context.Context, r ib.MatchedRequest) (reqId int64, repChan chan ib.Reply, err error) {
	var dataChan chan ib.Reply
	reqId, dataChan, err = p.sendMatchedRequest(ctx, r)
	if err != nil {
		return
	}

	// Cheap buffering to keep slow reply handling from blocking everything.
	// Eventually this should move to a system that dynamically appends an item to a
	// queue when repChan blocks.
	repChan = make(chan ib.Reply, 20)

	go func() {
		for {
			select {
			case data := <-dataChan:
				repChan <- data
			case <-ctx.Done():
				p.closeMatchedRequest(reqId)
				close(repChan)
				return

			}
		}
	}()

	return
}

func (p *IB) sendUnmatchedRequest(r ib.Request) error {
	return p.send(r)
}

func (p *IB) syncMatchedRequest(ctx context.Context, r ib.MatchedRequest, cb callbackFunc) (reqId int64, err error) {
	ctx, cancelFunc := context.WithCancel(ctx)
	reqId, dataChan, err := p.sendMatchedRequest(ctx, r)

	defer func() {
		cancelFunc()
		p.closeMatchedRequest(reqId)
	}()

	if err != nil {
		return reqId, err
	}

	for {
		select {
		case data := <-dataChan:
			if data == nil {
				return reqId, ErrConnectionClosed
			}

			behavior, err := cb(data)
			if err != nil || behavior == REPLY_DONE {
				return reqId, err
			}
		case <-ctx.Done():
			return reqId, ctx.Err()
		}
	}
}
