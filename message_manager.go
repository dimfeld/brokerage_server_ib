package brokerage_server_ib

import (
	"context"

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
	if idReply, ok := rep.(ib.MatchedReply); ok {
		id := idReply.ID()

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

func (p *IB) sendRequest(ctx context.Context, r ib.MatchedRequest, cb callbackFunc) error {
	nextId := p.engine.NextRequestID()
	r.SetID(nextId)

	dataChan := make(chan ib.Reply)
	p.pendingMutex.Lock()
	p.pending[nextId] = pendingReply{
		dataChan: dataChan,
		ctx:      ctx,
	}
	p.pendingMutex.Unlock()

	defer func() {
		p.pendingMutex.Lock()
		delete(p.pending, nextId)
		p.pendingMutex.Unlock()
	}()

	if err := p.engine.Send(r); err != nil {
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
