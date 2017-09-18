package brokerage_server_ib

import (
	"context"
	"sync/atomic"

	"github.com/nothize/ib"
)

type replyBehavior int

type pendingReply struct {
	dataChan chan ib.Reply
	ctx      context.Context
}

const (
	REPLY_CONTINUE replyBehavior = iota
	REPLY_DONE
)

type callbackFunc func(r ib.Reply) (replyBehavior, error)

func (p *IB) handleReply(rep ib.Reply) {
	switch r := rep.(type) {
	case *ib.NextValidID:
		atomic.StoreInt64(&p.nextOrderIdValue, r.OrderID)
		p.open = true
		if p.connectChan != nil {
			close(p.connectChan)
			p.connectChan = nil
		}

	case ib.MatchedReply:
		id := r.ID()

		p.pendingMutex.Lock()
		reply, ok := p.pending[id]
		p.pendingMutex.Unlock()

		if !ok {
			// Got an unexpected reply
			// TODO log something
			return
		}

		select {
		case reply.dataChan <- rep:
		case <-reply.ctx.Done():
		}
	}

	// TODO Handle unmatched replies.
}

func (p *IB) nextOrderID() int64 {
	return atomic.AddInt64(&p.nextOrderIdValue, 1)
}

func (p *IB) sendMatchedRequest(ctx context.Context, r ib.MatchedRequest) (int64, chan ib.Reply, error) {
	nextId := p.nextOrderID()
	r.SetID(nextId)

	dataChan := make(chan ib.Reply)
	p.pendingMutex.Lock()
	p.pending[nextId] = pendingReply{
		dataChan: dataChan,
		ctx:      ctx,
	}
	p.pendingMutex.Unlock()

	return nextId, dataChan, p.engine.Send(r)
}

func (p *IB) closeMatchedRequest(id int64) {
	p.pendingMutex.Lock()
	delete(p.pending, id)
	p.pendingMutex.Unlock()
}

func (p *IB) startStreamingRequest(ctx context.Context, r ib.MatchedRequest) (chan ib.Reply, error) {
	if err := p.engine.Send(r); err != nil {
		return nil, err
	}

	reqId, dataChan, err := p.sendMatchedRequest(ctx, r)
	if err != nil {
		return nil, err
	}

	repChan := make(chan ib.Reply, 20)

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

	return repChan, nil
}

func (p *IB) sendUnmatchedRequest(ctx context.Context, r ib.Request) error {
	return p.engine.Send(r)
}

func (p *IB) waitForMatchedRequest(ctx context.Context, r ib.MatchedRequest, cb callbackFunc) error {
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
			behavior, err := cb(data)
			if err != nil || behavior == REPLY_DONE {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
