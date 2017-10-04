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
		Symbol: ck.Symbol,
		// Would be nice to be able to always say SMART, but a few indexes like SPX don't appear there.
		// Exchange:     "SMART",
		SecurityType: ck.SecurityType,
		Expiry:       ck.Expiry,
		Multiplier:   ck.Multiplier,
		Strike:       ck.Strike,
		Right:        right,
		Currency:     "USD", // TODO Hardcoding this probably breaks non-US securities.
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

func ContractKeyFromOption(t *types.Option) ContractKey {
	return ContractKey{
		Symbol:       t.Underlying,
		SecurityType: "OPT",
		Expiry:       t.Expiration,
		Strike:       t.Strike,
		PutOrCall:    t.Type,
	}
}

type contractAndChannel struct {
	details []ib.ContractDetails
	pending chan struct{}
}

type contractManager struct {
	// Currently it holds on to these forever. I don't anticipate a problem here
	// but in very high usage situations it may become a memory hog.
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

	doneChan := make(chan struct{})
	newData := contractAndChannel{
		details: nil,
		pending: doneChan,
	}

	var existing contractAndChannel
	var ok bool

	// Add the initial entry with the pending channel
	cm.mutex.Lock()
	if existing, ok = cm.contracts[key]; !ok {
		cm.contracts[key] = newData
	}
	cm.mutex.Unlock()

	if ok {
		// This contract was already entered by another before we got here, so defer to that one.
		close(doneChan)
		return existing.pending
	}

	wg := &sync.WaitGroup{}

	fetch := func(outDetails *[]ib.ContractDetails, secType string) {
		defer wg.Done()

		var addedKeys []ContractKey
		seenContractIds := map[int64]bool{}

		thisKey := key
		thisKey.SecurityType = secType
		cm.engine.LogDebugNormal("Fetching contract details", "key", key)
		request := ib.RequestContractData{
			Contract: thisKey.ToContract(),
		}

		var thisDetails []ib.ContractDetails

		_, err := cm.engine.syncMatchedRequest(ctx, &request, func(mr ib.Reply) (replyBehavior, error) {
			switch r := mr.(type) {
			case *ib.ContractData:
				newKey := NewContractKey(&r.Contract.Summary)

				conID := r.Contract.Summary.ContractID
				if seenContractIds[conID] {
					// Don't include contract IDs we've already seen. This generally occurs when
					// we get multiple pieces of data identical except for the exchange.
					break
				}
				seenContractIds[conID] = true

				if newKey != thisKey {
					// The two keys are different, so add the data at the original key as well.
					// This happens when the original contract key is ambiguous such as when fetching
					// option chains.
					cm.mutex.Lock()
					cac := cm.contracts[newKey]
					cac.details = append(cac.details, r.Contract)
					cac.pending = doneChan
					cm.contracts[newKey] = cac
					cm.mutex.Unlock()

					cm.engine.LogDebugVerbose("Adding additional contract details", "key", newKey, "data", r.Contract)
					addedKeys = append(addedKeys, newKey)
				}

				thisDetails = append(thisDetails, r.Contract)

			case *ib.ContractDataEnd:
				return REPLY_DONE, nil

			case *ib.ErrorMessage:
				if r.Code == IBErrSymbolNotFound {
					// Don't treat this as an error since in the IND+STK case we'll always have
					// at least one of them fail. It's handled later when GetContractDetails checks
					// that there's at least 1 item in the array.
					return REPLY_DONE, nil
				}
				return REPLY_DONE, r.Error()
			}

			return REPLY_CONTINUE, nil
		})

		if len(addedKeys) > 0 {
			cm.mutex.Lock()
			for _, added := range addedKeys {
				// Remove the pending channel from all the other keys that we added too, if any.
				cac := cm.contracts[added]
				cac.pending = nil
				cm.contracts[added] = cac
			}
			cm.mutex.Unlock()
		}

		if err == nil {
			*outDetails = thisDetails
		} else {
			cm.engine.Logger.Error("fetchContractDetails error", "key", key, "err", err.Error())
		}
	}

	var detailsOne []ib.ContractDetails
	var detailsTwo []ib.ContractDetails
	if key.SecurityType == "" {
		wg.Add(2)
		go fetch(&detailsOne, "STK")
		go fetch(&detailsTwo, "IND")
	} else {
		wg.Add(1)
		go fetch(&detailsOne, key.SecurityType)
	}

	go func() {
		wg.Wait()

		// Once everything is done, copy the final data into the cache.
		newData.pending = nil

		if detailsOne != nil && detailsTwo != nil {
			// This will probably never happen, but handle it just in case.
			newData.details = append(detailsOne, detailsTwo...)
		} else if detailsOne != nil {
			newData.details = detailsOne
		} else if detailsTwo != nil {
			newData.details = detailsTwo
		}
		// Else both are nil. Nothing to do.

		cm.mutex.Lock()
		cm.contracts[key] = newData
		close(doneChan)
		cm.mutex.Unlock()

		cm.engine.LogDebugVerbose("Finished retrieving contract details", "key", key, "data", newData.details)
	}()

	return doneChan
}

func (cm *contractManager) GetContractDetails(ctx context.Context, key ContractKey) ([]ib.ContractDetails, error) {
	cm.engine.LogDebugVerbose("GetContractDetails", "key", key)

	cm.mutex.RLock()
	cac := cm.contracts[key]
	cm.mutex.RUnlock()

	if cac.details == nil {
		if cac.pending != nil {
			// There's already an outstanding fetch, so just wait
			cm.engine.LogDebugVerbose("Waiting for existing request", "key", key)
			<-cac.pending
		} else {
			// Start the fetch. This returns the channel to wait on so we don't have to check the
			// map again yet.
			<-cm.fetchContractDetails(ctx, key)
		}

		cm.mutex.RLock()
		cac = cm.contracts[key]
		cm.mutex.RUnlock()
		cm.engine.LogDebugTrace("GetContractDetails done", "key", key, "data", cac.details)
	} else {
		cm.engine.LogDebugTrace("Data from cache", "key", key, "data", cac.details)
	}

	if len(cac.details) == 0 {
		cm.engine.LogDebugNormal("No contract details found", "key", key)
		return nil, ErrContractNotFound
	}

	return cac.details, nil
}
