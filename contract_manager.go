package brokerage_server_ib

import (
	"context"
	"errors"
	"sync"

	"github.com/dimfeld/brokerage_server/types"
	"github.com/dimfeld/ib"
)

var ErrContractNotFound = errors.New("Contract not found or is ambiguous")

type ContractKey struct {
	Symbol       string
	SecurityType string
	Expiry       string
	Multiplier   string
	Strike       float64
	PutOrCall    types.PutOrCall
}

func (ck ContractKey) ToContract() ib.Contract {
	var right string
	if ck.SecurityType == "OPT" {
		if ck.PutOrCall == types.Put {
			right = "P"
		} else {
			right = "C"
		}
	}

	return ib.Contract{
		Symbol:       ck.Symbol,
		SecurityType: ck.SecurityType,
		Expiry:       ck.Expiry,
		Multiplier:   ck.Multiplier,
		Strike:       ck.Strike,
		Right:        right,
		Currency:     "USD",
	}
}

func NewContractKey(c *ib.Contract) ContractKey {
	var pc types.PutOrCall
	if c.SecurityType == "OPT" {
		if c.Right == "P" || c.Right == "PUT" {
			pc = types.Put
		} else {
			pc = types.Call
		}
	}
	return ContractKey{
		Symbol:       c.Symbol,
		SecurityType: c.SecurityType,
		Expiry:       c.Expiry,
		Multiplier:   c.Multiplier,
		Strike:       c.Strike,
		PutOrCall:    pc,
	}
}

type contractAndChannel struct {
	details []ib.ContractDetails
	pending chan struct{}
}

type contractManager struct {
	contracts map[ContractKey]contractAndChannel

	mutex   *sync.RWMutex
	pending *sync.Cond
	engine  *IB
}

func newContractManager(p *IB) *contractManager {
	return &contractManager{
		contracts: map[ContractKey]contractAndChannel{},
		mutex:     &sync.RWMutex{},
		engine:    p,
	}
}

func (cm *contractManager) fetchContractDetails(ctx context.Context, key ContractKey) chan struct{} {
	request := ib.RequestContractData{
		Contract: key.ToContract(),
	}

	doneChan := make(chan struct{})
	newData := contractAndChannel{
		details: make([]ib.ContractDetails, 0, 1),
		pending: doneChan,
	}

	cm.mutex.Lock()
	cm.contracts[key] = newData
	cm.mutex.Unlock()

	var addedKeys []ContractKey

	err := cm.engine.syncMatchedRequest(ctx, &request, func(mr ib.Reply) (replyBehavior, error) {
		switch r := mr.(type) {
		case *ib.ContractData:
			newKey := NewContractKey(&r.Contract.Summary)

			if newKey != key {
				// The two keys are different, so add the data at the original key as well.
				// This happens when the original contract key is ambiguous such as when fetching
				// option chains.
				cm.mutex.Lock()
				cac := cm.contracts[newKey]
				cac.details = append(cac.details, r.Contract)
				cac.pending = doneChan
				cm.contracts[newKey] = cac
				cm.mutex.Unlock()
				addedKeys = append(addedKeys, newKey)
			}

			newData.details = append(newData.details, r.Contract)

		case *ib.ContractDataEnd:
			return REPLY_DONE, nil

		case *ib.ErrorMessage:
			return REPLY_DONE, r.Error()
		}

		return REPLY_CONTINUE, nil
	})

	newData.pending = nil
	cm.mutex.Lock()
	cm.contracts[key] = newData

	for _, added := range addedKeys {
		// Remove the pending channel from all the other keys that we added too, if any.
		cac := cm.contracts[added]
		cac.pending = nil
		cm.contracts[added] = cac
	}

	close(doneChan)
	cm.mutex.Unlock()

	if err != nil {
		cm.engine.Logger.Error("error while fetching contract", "key", key, "err", err.Error())
	}

	return doneChan
}

func (cm *contractManager) GetContractDetails(ctx context.Context, key ContractKey) ([]ib.ContractDetails, error) {
	cm.mutex.RLock()
	cac := cm.contracts[key]
	cm.mutex.RUnlock()

	if cac.details == nil {
		if cac.pending != nil {
			// There's already an outstanding fetch, so just wait
			<-cac.pending
		} else {
			// Start the fetch. This returns the channel to wait on so we don't have to check the
			// map again yet.
			<-cm.fetchContractDetails(ctx, key)
		}

		cm.mutex.RLock()
		cac = cm.contracts[key]
		cm.mutex.RUnlock()
	}

	if len(cac.details) == 0 {
		return nil, ErrContractNotFound
	}

	return cac.details, nil
}
