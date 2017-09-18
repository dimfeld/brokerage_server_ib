package brokerage_server_ib

import "github.com/nothize/ib"

func (p *IB) runEventLoop() chan struct{} {
	stateChan := make(chan ib.EngineState)
	replyChan := make(chan ib.Reply)
	doneChan := make(chan struct{})

	go func() {
		for {
			select {
			case state := <-stateChan:
				p.handleState(state)
			case reply := <-replyChan:
				p.handleReply(reply)
			case <-doneChan:
				// All done.
				return
			}
		}
	}()

	return doneChan
}

func (p *IB) handleState(ib.EngineState) {
	// TODO Handle disconnection, somehow.
}
