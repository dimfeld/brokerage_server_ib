package brokerage_server_ib

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/dimfeld/ib"
	"github.com/inconshreveable/log15"
	jsoniter "github.com/json-iterator/go"

	"github.com/dimfeld/brokerage_server/types"
)

const (
	DEFAULT_GATEWAY      = "localhost:7497"
	MAX_SENDS_PER_SECOND = 50
	BrokerName           = "ib"
)

type IB struct {
	engine        *ib.Engine
	engineOptions ib.EngineOptions
	open          bool

	nextOrderIdValue int64

	contractManager *contractManager
	rateLimiter     *rate.Limiter

	optionMetaCacheMutex *sync.Mutex
	optionMetaCache      map[string]*types.OptionChain

	doneChan    chan struct{}
	connectChan chan error

	active      map[int64]activeReply
	activeMutex *sync.Mutex

	accountData      map[string]types.Account
	accountDataMutex *sync.Mutex

	Accounts []string

	// Debug logging level
	Debug   types.DebugLevel
	Timeout time.Duration
	Logger  log15.Logger
}

func (p *IB) LogDebugNormal(msg string, args ...interface{}) {
	if p.Debug >= types.DEBUG_NORMAL {
		p.Logger.Debug(msg, args...)
	}
}

func (p *IB) LogDebugVerbose(msg string, args ...interface{}) {
	if p.Debug >= types.DEBUG_VERBOSE {
		p.Logger.Debug(msg, args...)
	}
}

func (p *IB) LogDebugTrace(msg string, args ...interface{}) {
	if p.Debug >= types.DEBUG_TRACE {
		p.Logger.Debug(msg, args...)
	}
}

func (p *IB) SetDebugLevel(level types.DebugLevel) {
	p.Debug = level
}

func (p *IB) connect() (err error) {
	p.closeAllRequests()

	p.LogDebugNormal("Connecting")
	if p.engine, err = ib.NewEngine(p.engineOptions); err != nil {
		p.engine = nil
		if strings.HasSuffix(err.Error(), "connection refused") {
			err = fmt.Errorf("%s. Are you running TWS?", err.Error())
		}
		return err
	}

	p.contractManager = newContractManager(p)

	p.LogDebugVerbose("Created engine")

	p.runEventLoop()

	timeout := time.NewTimer(p.Timeout)
	select {
	case <-p.connectChan:
		if !timeout.Stop() {
			<-timeout.C
		}
	case <-timeout.C:
		return errors.New("Connection timeout")
	}

	return p.sendInitialSetup()
}

func (p *IB) sendInitialSetup() error {
	// Enable frozen data mode so that we can still get data after hours.
	// This still sends up-to-date real-time data during trading hours.
	dataTypeReq := &ib.RequestMarketDataType{
		MarketDataType: ib.MarketDataTypeFrozen,
	}
	if err := p.sendUnmatchedRequest(dataTypeReq); err != nil {
		return err
	}

	if err := p.accountDataLoop(); err != nil {
		return err
	}

	return nil
}

func (p *IB) Connect() (err error) {

	if p.Status().Connected {
		// Don't reconnect if we don't need to
		return nil
	}

	return p.connect()
}

func (p *IB) closeAllRequests() {
	p.activeMutex.Lock()
	for id := range p.active {
		p.closeMatchedRequestWithoutMutex(id)
	}
	p.activeMutex.Unlock()
}

func (p *IB) Close() error {
	p.open = false

	p.closeAllRequests()

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

func New(logger log15.Logger, config json.RawMessage) (*IB, error) {
	options := ib.EngineOptions{
		Client:  jsoniter.Get(config, "client_id").ToInt64(),
		Gateway: jsoniter.Get(config, "gateway").ToString(),
	}

	if options.Gateway == "" {
		options.Gateway = DEFAULT_GATEWAY
	}

	timeout := jsoniter.Get(config, "timeout").ToInt()
	if timeout == 0 {
		timeout = 5000
	}

	debug := types.DebugLevel(jsoniter.Get(config, "debug").ToInt())
	if debug > 3 {
		options.DumpConversation = true
	}

	return &IB{
		engineOptions: options,
		Debug:         debug,
		active:        map[int64]activeReply{},
		activeMutex:   &sync.Mutex{},
		Logger:        logger.New("plugin", "ib"),
		Timeout:       time.Duration(timeout) * time.Millisecond,
		rateLimiter:   rate.NewLimiter(MAX_SENDS_PER_SECOND, MAX_SENDS_PER_SECOND),

		accountData:      map[string]types.Account{},
		accountDataMutex: &sync.Mutex{},

		optionMetaCache:      map[string]*types.OptionChain{},
		optionMetaCacheMutex: &sync.Mutex{},
	}, nil
}
