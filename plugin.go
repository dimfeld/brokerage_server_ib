package brokerage_server_ib

import (
	"github.com/nothize/ib"

	"github.com/dimfeld/brokerage_server/types"
)

const DEFAULT_GATEWAY = "localhost:7497"

type IB struct {
	engine        *ib.Engine
	engineOptions ib.EngineOptions
	open          bool

	// Debug logging level
	Debug int
}

func (p *IB) connect() (err error) {
	if p.engine, err = ib.NewEngine(p.engineOptions); err != nil {
		p.engine = nil
		return err
	}

	p.open = true
	return nil
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
	p.engine.Stop()
	p.engine = nil
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

	debug, _ := config["debug"].(int)

	return &IB{
		engineOptions: options,
		Debug:         debug,
	}, nil
}
