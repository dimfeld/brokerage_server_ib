package brokerage_server_ib

import (
	"errors"
	"sync"
	"time"

	"github.com/nothize/ib"

	"github.com/dimfeld/brokerage_server/types"
)

const DEFAULT_GATEWAY = "localhost:7497"

type IB struct {
	engine        *ib.Engine
	engineOptions ib.EngineOptions
	open          bool

	nextOrderIdValue int64

	doneChan    chan struct{}
	connectChan chan error

	pending      map[int64]pendingReply
	pendingMutex *sync.Mutex

	// Debug logging level
	Debug   int
	Timeout time.Duration
}

func (p *IB) connect() (err error) {
	if p.engine, err = ib.NewEngine(p.engineOptions); err != nil {
		p.engine = nil
		return err
	}

	if p.doneChan != nil {
		close(p.doneChan)
	}

	p.runEventLoop()

	timeout := time.NewTimer(p.Timeout)
	select {
	case <-p.connectChan:
		if !timeout.Stop() {
			<-timeout.C
		}
		return nil
	case <-timeout.C:
		return errors.New("Connection timeout")
	}
}

func (p *IB) Connect() (err error) {

	if p.Status().Connected {
		// Don't reconnect if we don't need to
		return nil
	}

	return p.connect()
}

func (p *IB) Close() error {
	p.open = false

	if p.engine != nil {
		p.engine.Stop()
		p.engine = nil
	}

	if p.doneChan != nil {
		close(p.doneChan)
	}

	return nil
}

func (p *IB) Status() *types.ConnectionStatus {
	if !p.open || p.engine == nil {
		return &types.ConnectionStatus{
			Connected: false,
			Error:     nil,
		}
	}

	return &types.ConnectionStatus{
		Connected: p.engine.State() == ib.EngineReady,
		Error:     p.engine.FatalError(),
	}
}

func (p *IB) Error() error {
	if p.engine != nil {
		return p.engine.FatalError()
	}

	return nil
}

func New(config map[string]interface{}) (*IB, error) {
	options := ib.EngineOptions{}
	var ok bool

	if options.Gateway, ok = config["gateway"].(string); !ok {
		options.Gateway = DEFAULT_GATEWAY
	}

	if clientId, ok := config["client_id"].(int); ok {
		options.Client = int64(clientId)
	}

	debug, _ := config["debug"].(int)

	return &IB{
		engineOptions: options,
		Debug:         debug,
		pending:       map[int64]pendingReply{},
		pendingMutex:  &sync.Mutex{},
	}, nil
}
