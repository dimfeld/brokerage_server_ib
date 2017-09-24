package brokerage_server_ib

import "github.com/dimfeld/ib"

func (p *IB) runEventLoop() {
	stateChan := make(chan ib.EngineState)
	replyChan := make(chan ib.Reply)
	p.doneChan = make(chan struct{})
	p.connectChan = make(chan error)

	go func() {
		for {
			select {
			case state := <-stateChan:
				p.handleState(state)
			case reply := <-replyChan:
				p.handleReply(reply)
			case <-p.doneChan:
				// All done.
				return
			}
		}
	}()

	p.engine.SubscribeAll(replyChan)
	p.engine.SubscribeState(stateChan)
}

func (p *IB) handleState(ib.EngineState) {
	// TODO Handle disconnection, somehow.
}
