package brokerage_server_ib

import "github.com/nothize/ib"

type replyBehavior int

const (
	REPLY_SEND_AND_CLOSE replyBehavior = iota
	REPLY_SEND_AND_CONTINUE
	REPLY_DROP_AND_CLOSE
	REPLY_DROP_AND_CONTINUE
)

type replyBehaviorFunc func(r ib.Reply) replyBehavior

type pendingReply struct {
	ch       chan<- ib.Reply
	behavior replyBehaviorFunc
}

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

		// If there's no behavior function, then assume there's just one message.
		behavior := REPLY_SEND_AND_CLOSE
		if reply.behavior != nil {
			behavior = reply.behavior(rep)
		}

		switch behavior {
		case REPLY_SEND_AND_CLOSE:
			// All done, and the client should receive this message.
			reply.ch <- rep
			close(reply.ch)

			p.pendingMutex.Lock()
			delete(p.pending, id)
			p.pendingMutex.Unlock()
		case REPLY_SEND_AND_CONTINUE:
			// Pass the message along, and leave the pipe open for future messages.
			reply.ch <- rep
		case REPLY_DROP_AND_CLOSE:
			// We don't need to send this message, but it does indicate that the reply is done.
			close(reply.ch)
			p.pendingMutex.Lock()
			delete(p.pending, id)
			p.pendingMutex.Unlock()
		case REPLY_DROP_AND_CONTINUE:
			// Don't need to pass this message along, bbut we're not at the end. Just continue on.
		}
	}

	// TODO Handle unmatched replies.
}

func (p *IB) sendRequest(r ib.MatchedRequest, behavior replyBehaviorFunc) (chan<- ib.Reply, error) {
	nextId := p.engine.NextRequestID()
	r.SetID(nextId)
	if err := p.engine.Send(r); err != nil {
		return nil, nil
	}

	data := &pendingReply{
		ch:       make(chan<- ib.Reply, 1),
		behavior: behavior,
	}

	p.pendingMutex.Lock()
	p.pending[nextId] = data
	p.pendingMutex.Unlock()

	return data.ch, nil
}
