package brokerage_server_ib

import (
	"context"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/dimfeld/brokerage_server/types"
	"github.com/dimfeld/ib"
)

var (
	optionHistTypes = []ib.HistDataToShow{ib.HistBid, ib.HistAsk, ib.HistTrades}
)

func (p *IB) GetOptionsChain(ctx context.Context, underlying string) (types.OptionChain, error) {

	p.optionMetaCacheMutex.Lock()
	output := p.optionMetaCache[underlying]
	p.optionMetaCacheMutex.Unlock()

	if output != nil {
		return *output, nil
	}

	key := ContractKey{
		Symbol: underlying,
	}
	contractDetails, err := p.contractManager.GetContractDetails(ctx, key)
	if err != nil {
		return types.OptionChain{}, err
	}

	p.LogDebugNormal("Got contract details", "data", contractDetails)

	contract := contractDetails[0].Summary
	request := &ib.RequestSecDefOptParams{
		Symbol:     contract.Symbol,
		SecType:    contract.SecurityType,
		ContractId: contract.ContractID,
		// No exchange, since the options trade on a different exchange than the underlying.
		Exchange: "",
	}

	expirations := map[string]bool{}
	strikes := map[float64]bool{}
	exchanges := []string{}
	var multiplier string

	_, err = p.syncMatchedRequest(ctx, request, func(r ib.Reply) (replyBehavior, error) {
		switch data := r.(type) {
		case *ib.SecurityDefinitionOptionParameter:

			for _, value := range data.Expirations {
				expirations[value] = true
			}

			for _, value := range data.Strikes {
				strikes[value] = true
			}

			exchanges = append(exchanges, data.Exchange)
			multiplier = data.Multiplier // I think multiplier is always the same?
			return REPLY_CONTINUE, nil
		case *ib.SecurityDefinitionOptionParameterEnd:
			return REPLY_DONE, nil
		case *ib.ErrorMessage:
			if data.Code == IBErrSymbolNotFound {
				return REPLY_DONE, types.ErrSymbolNotFound
			} else {
				return REPLY_DONE, data.Error()
			}
		}

		return REPLY_CONTINUE, nil
	})

	if err != nil {
		return types.OptionChain{}, nil
	}

	strikesList := make([]float64, 0, len(strikes))
	for s := range strikes {
		strikesList = append(strikesList, s)
	}

	expiresList := make([]string, 0, len(expirations))
	for e := range expirations {
		expiresList = append(expiresList, e)
	}

	sort.Strings(expiresList)
	sort.Float64s(strikesList)

	output = &types.OptionChain{
		Underlying:  underlying,
		Multiplier:  multiplier,
		Strikes:     strikesList,
		Expirations: expiresList,
	}

	p.optionMetaCacheMutex.Lock()
	p.optionMetaCache[underlying] = output
	p.optionMetaCacheMutex.Unlock()

	return *output, err
}

func (p *IB) getOneOptionQuote(ctx context.Context, contract *ib.Contract) (*types.OptionQuote, error) {

	output := &types.OptionQuote{
		Rho: -1, // IB doesn't return Rho
	}

	wg := &sync.WaitGroup{}

	var mktDataErr error
	getMktData := func() {
		defer wg.Done()

		// TODO timer should be configurable.
		ctx, cancelFunc := context.WithTimeout(ctx, time.Duration(6)*time.Second)
		defer cancelFunc()

		req := &ib.RequestMarketData{
			Contract:        *contract,
			GenericTickList: "101",
			Snapshot:        false,
		}

		optionTicks := make([]*ib.TickOptionComputation, 4)

		seen := map[int64]bool{}
		const needed = 7

		reqId, err := p.syncMatchedRequest(ctx, req, func(r ib.Reply) (replyBehavior, error) {
			switch data := r.(type) {
			case *ib.TickGeneric:
				countSeen := true
				switch data.Type {

				default:
					countSeen = false
				}

				if countSeen {
					seen[data.Type] = true
				}

			case *ib.TickOptionComputation:
				// Save the ticks in an array. These are ordered with the best first.
				// The only one that we really want/need is the model computation
				// but the others will do if the model doesn't arrive in a timely manner.
				switch data.Type {

				case ib.TickModelOption:
					optionTicks[0] = data
					output.ModelPrice = data.OptionPrice
					seen[data.Type] = true
				case ib.TickLastOptionComputation:
					optionTicks[1] = data
				case ib.TickBidOptionComputation:
					optionTicks[2] = data
				case ib.TickAskOptionComputation:
					optionTicks[3] = data
				}
			case *ib.TickSize:
				if data.Size > 0 {
					switch data.Type {
					case ib.TickOptionCallOpenInt:
						output.OpenInterest = data.Size
					case ib.TickOptionPutOpenInt:
						output.OpenInterest = data.Size
					case ib.TickVolume:
						output.Volume = data.Size
					}
				}
			case *ib.TickPrice:
				countSeen := true
				switch data.Type {
				case ib.TickBid:
					output.Bid = data.Price
					output.BidSize = data.Size
				case ib.TickAsk:
					output.Ask = data.Price
					output.AskSize = data.Size
				case ib.TickClose:
					output.Close = data.Price
				case ib.TickLast:
					output.Last = data.Price
					output.LastSize = data.Size
					countSeen = false // TODO Not sure yet if this comes through during trading hours
				default:
					countSeen = false
				}

				if countSeen {
					seen[data.Type] = true
				}

			case *ib.TickString:
				usedValue := true
				switch data.Type {
				// case ib.TickAskExch:
				// 	output.AskExch = tick.Value
				// case ib.TickBidExch:
				// 	output.BidExch = tick.Value
				// case ib.TickLastExchange:
				// 	output.LastExch = tick.Value
				case ib.TickLastTimestamp:
					if t, err := strconv.ParseInt(data.Value, 10, 64); err == nil {
						output.LastTime = time.Unix(int64(t), 0)
					}
				default:
					usedValue = false
				}

				if usedValue {
					seen[data.Type] = true
				}

			case *ib.TickSnapshotEnd:
				output.Incomplete = true
				return REPLY_DONE, nil
			case *ib.ErrorMessage:
				return REPLY_DONE, data.Error()
			}

			if len(seen) >= needed {
				return REPLY_DONE, nil
			}
			return REPLY_CONTINUE, nil
		})

		if err == nil && err != context.DeadlineExceeded {
			mktDataErr = err
		}

		// Make sure to cancel the stream once we're all done.
		cancelReq := ib.CancelMarketData{}
		cancelReq.SetID(reqId)
		p.sendUnmatchedRequest(&cancelReq)

		// Assign common data from the ticks we received. They are already in priority order.
		for _, data := range optionTicks {
			if data == nil {
				continue
			}

			output.Delta = data.Delta
			output.Gamma = data.Gamma
			output.Theta = data.Theta
			output.Vega = data.Vega
			output.OptionImpliedVolatility = data.ImpliedVol
			break // Once we find one, no need to keep going.
		}
	}

	// var histData []*ib.HistoricalData
	// var histErr error
	// getHistData := func() {
	// 	defer wg.Done()
	// 	histData, histErr = p.getLatestHistData(ctx, contract, optionHistTypes)
	// }

	var contractDetails *ib.ContractDetails
	var contractErr error
	getContractDetails := func() {
		defer wg.Done()
		details, err := p.contractManager.GetContractDetails(ctx, NewContractKey(contract))
		if err == nil {
			contractDetails = &details[0]
		}
	}

	wg.Add(2)
	go getMktData()
	go getContractDetails()
	// TODO probably dont need to fetch historical data during trading hours. I think?
	// go getHistData()

	output.Underlying = contract.Symbol
	output.Strike = contract.Strike
	output.Expiration = contract.Expiry
	output.Time = time.Now()
	if contract.Right[0] == 'P' {
		output.Type = types.Put
	} else {
		output.Type = types.Call
	}

	wg.Wait()

	if contractErr != nil {
		return output, contractErr
	} else if mktDataErr != nil {
		return output, mktDataErr
		// } else if histErr != nil {
		// 	return output, histErr
	}

	output.MinPriceDelta = contractDetails.MinTick
	output.FullSymbol = contractDetails.Summary.LocalSymbol

	// for i, data := range histData {
	// 	if data == nil {
	// 		continue
	// 	}

	// 	items := data.Data
	// 	if len(items) == 0 {
	// 		continue
	// 	}

	// 	item := &items[0]

	// 	switch optionHistTypes[i] {
	// 	case ib.HistAsk:
	// 		if output.Ask <= 0 || output.Ask >= 99999 {
	// 			output.Ask = item.Close
	// 		}
	// 	case ib.HistBid:
	// 		if output.Bid <= 0 || output.Bid >= 99999 {
	// 			output.Bid = item.Close
	// 		}
	// 	case ib.HistTrades:
	// 		if output.Last <= 0 || output.Last >= 99999 {
	// 			output.Last = item.Close
	// 		}
	// 	}
	// }

	return output, nil
}

func (p *IB) GetOptionsQuotes(ctx context.Context, params types.OptionsQuoteParams) ([]*types.OptionQuote, error) {

	p.LogDebugNormal("GetOptionsQuotes", "params", params)

	details, err := p.contractManager.GetContractDetails(ctx, ContractKey{Symbol: params.Underlying})
	if err != nil {
		return nil, err
	}

	numNeeded := len(params.Expirations) * len(params.Strikes)
	if params.Puts && params.Calls {
		numNeeded *= 2
	}

	if !params.Puts && !params.Calls {
		params.Puts = true
		params.Calls = true
	}

	contracts := make([]ib.Contract, 0, numNeeded)

	detail := &details[0].Summary
	contract := ib.Contract{
		Symbol:       detail.Symbol,
		SecurityType: "OPT",
		Exchange:     "SMART",
	}

	for _, expiration := range params.Expirations {
		contract.Expiry = expiration

		for _, strike := range params.Strikes {
			contract.Strike = strike

			if params.Puts {
				contract.Right = "P"
				contracts = append(contracts, contract)
			}

			if params.Calls {
				contract.Right = "C"
				contracts = append(contracts, contract)
			}
		}
	}

	output := make([]*types.OptionQuote, len(contracts))
	errs := make([]error, len(contracts))
	wg := &sync.WaitGroup{}
	wg.Add(len(contracts))

	for i := range contracts {
		go func(i int) {
			output[i], errs[i] = p.getOneOptionQuote(ctx, &contracts[i])
			wg.Done()
		}(i)
	}

	wg.Wait()

	return output, nil

}
