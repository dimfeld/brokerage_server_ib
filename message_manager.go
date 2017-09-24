package brokerage_server_ib

import (
	"context"
	"errors"
	"io"
	"sync/atomic"

	"github.com/dimfeld/brokerage_server/types"
	"github.com/dimfeld/ib"
)

type replyBehavior int

type activeReply struct {
	dataChan chan ib.Reply
	ctx      context.Context
}

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
		p.LogDebugVerbose("Unexpected reply", "msg", r)
		return
	}

	select {
	case reply.dataChan <- r:
	case <-reply.ctx.Done():
	}
}

func (p *IB) handleReply(rep ib.Reply) {
	p.LogDebugTrace("received", "msg", rep)

	switch r := rep.(type) {
	case *ib.ManagedAccounts:
		// TODO Save list of accounts

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

		if r.SeverityWarning() {
			p.Logger.Warn("info", "err", r)
		} else {
			p.Logger.Error("received error", "err", r)
		}

		// TODO Some errors are not actually replies to the
		// request, and should be handled here instead of passed
		// through.

		if id != -1 {
			p.handleMatchedReply(r)
			// Make sure the request gets closed, since nothing else is coming in.
			p.closeMatchedRequest(id)
		}

	case ib.MatchedReply:
		p.handleMatchedReply(r)
	}

	// TODO Handle unmatched replies.
}

func (p *IB) nextOrderID() int64 {
	return atomic.AddInt64(&p.nextOrderIdValue, 1)
}

func (p *IB) send(r ib.Request) error {
	p.LogDebugTrace("Sending", "msg", r)
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

func (p *IB) closeMatchedRequest(id int64) {
	p.activeMutex.Lock()
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
	}
	p.activeMutex.Unlock()
}

func (p *IB) startStreamingRequest(ctx context.Context, r ib.MatchedRequest) (reqId int64, repChan chan ib.Reply, err error) {
	if err = p.send(r); err != nil {
		return
	}

	var dataChan chan ib.Reply
	reqId, dataChan, err = p.sendMatchedRequest(ctx, r)
	if err != nil {
		return
	}

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

func (p *IB) syncMatchedRequest(ctx context.Context, r ib.MatchedRequest, cb callbackFunc) error {
	ctx, cancelFunc := context.WithCancel(ctx)
	reqId, dataChan, err := p.sendMatchedRequest(ctx, r)

	defer func() {
		cancelFunc()
		p.closeMatchedRequest(reqId)
	}()

	if err != nil {
		return err
	}

	for {
		select {
		case data := <-dataChan:
			if data == nil {
				// TODO Better error handling
				return errors.New("error occurred")
			}

			behavior, err := cb(data)
			if err != nil || behavior == REPLY_DONE {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
