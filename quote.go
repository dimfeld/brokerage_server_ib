package brokerage_server_ib

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/dimfeld/brokerage_server/types"
	"github.com/dimfeld/ib"
)

var (
	equityHistDataTypes = []ib.HistDataToShow{ib.HistBid, ib.HistAsk, ib.HistTrades, ib.HistVolatility, ib.HistOptionIV}
)

func (p *IB) GetStockQuote(ctx context.Context, symbol string) (*types.Quote, error) {
	const quoteFieldCount = 18

	key := ContractKey{Symbol: symbol}
	details, err := p.contractManager.GetContractDetails(ctx, key)
	if err != nil {
		return nil, err
	}

	contract := &details[0].Summary

	req := &ib.RequestMarketData{
		Contract: *contract,
		// Option Volume, Open Interest, HV, IV, Mark Price
		GenericTickList: "100,101,104,106,221",
		// Generic ticks only work if you request it as streaming data.
		Snapshot: false,
	}

	output := &types.Quote{
		Time: time.Now(),
	}
	seen := map[int64]bool{}

	// We don't get the "end" message for 11 seconds which is way too long in the case
	// where IB doesn't give us all the data. 3 seconds is more than enough time to get
	// the data in my experience, while still seeming somewhat responsive. The historical
	// data will fill in the rest.
	// TODO timer should be configurable.
	ctx, cancelFunc := context.WithTimeout(ctx, time.Duration(3)*time.Second)
	defer cancelFunc()

	// TODO probably dont need to fetch historical data during trading hours. I think?
	var histDataItems []*ib.HistoricalData
	var histDataErr error
	var histDataWg sync.WaitGroup
	histDataWg.Add(1)
	go func() {
		histDataItems, histDataErr = p.getLatestHistData(ctx, contract, equityHistDataTypes)
		histDataWg.Done()
	}()

	reqId, err := p.syncMatchedRequest(ctx, req, func(r ib.Reply) (replyBehavior, error) {
		switch tick := r.(type) {
		case *ib.TickGeneric:
			usedValue := true
			switch tick.Type {
			case ib.TickOptionImpliedVol:
				output.OptionImpliedVolatility = tick.Value
			case ib.TickOptionHistoricalVol:
				output.OptionHistoricalVolatility = tick.Value
			default:
				usedValue = false
			}

			if usedValue {
				seen[tick.Type] = true
			}

		case *ib.TickPrice:
			usedValue := true
			switch tick.Type {
			case ib.TickAsk:
				output.Ask = tick.Price
				output.AskSize = tick.Size
				seen[ib.TickAskSize] = true
			case ib.TickBid:
				output.Bid = tick.Price
				output.BidSize = tick.Size
				seen[ib.TickBidSize] = true
			case ib.TickLast:
				output.Last = tick.Price
				output.LastSize = tick.Size
				seen[ib.TickLastSize] = true
			case ib.TickMarkPrice:
				output.Mark = tick.Price
			case ib.TickHigh:
				output.High = tick.Price
			case ib.TickLow:
				output.Low = tick.Price
			case ib.TickOpen:
				output.Open = tick.Price
			case ib.TickClose:
				output.Close = tick.Price
			default:
				usedValue = false
			}

			if usedValue {
				seen[tick.Type] = true
			}

		case *ib.TickSize:
			usedValue := true
			switch tick.Type {
			case ib.TickAskSize:
				output.AskSize = tick.Size
			case ib.TickBidSize:
				output.BidSize = tick.Size
			case ib.TickLastSize:
				output.LastSize = tick.Size
			case ib.TickVolume:
				output.Volume = tick.Size * 100
			case ib.TickOptionCallOpenInt:
				output.OptionCallOpenInt = tick.Size
			case ib.TickOptionPutOpenInt:
				output.OptionPutOpenInt = tick.Size
			case ib.TickOptionCallVolume:
				output.OptionCallVolume = tick.Size
			case ib.TickOptionPutVolume:
				output.OptionPutVolume = tick.Size
			default:
				usedValue = false
			}

			if usedValue {
				seen[tick.Type] = true
			}

		case *ib.TickString:
			usedValue := true
			switch tick.Type {
			// case ib.TickAskExch:
			// 	output.AskExch = tick.Value
			// case ib.TickBidExch:
			// 	output.BidExch = tick.Value
			// case ib.TickLastExchange:
			// 	output.LastExch = tick.Value
			case ib.TickLastTimestamp:
				if t, err := strconv.ParseInt(tick.Value, 10, 64); err == nil {
					tim := time.Unix(int64(t), 0)
					output.LastTime = &tim
				}
			default:
				usedValue = false
			}

			if usedValue {
				seen[tick.Type] = true
			}

		case *ib.TickSnapshotEnd:
			// We should usually finish before receiving this, but handle
			// it just in case.
			p.LogDebugNormal("quote exited early", "seen", seen)
			return REPLY_DONE, nil

		case *ib.ErrorMessage:
			if tick.Code == IBErrSymbolNotFound {
				return REPLY_DONE, types.ErrSymbolNotFound
			} else {
				return REPLY_DONE, tick.Error()
			}
		}

		if len(seen) >= quoteFieldCount {
			return REPLY_DONE, nil
		}

		return REPLY_CONTINUE, nil
	})

	// Make sure to cancel the stream once we're all done.
	cancelReq := ib.CancelMarketData{}
	cancelReq.SetID(reqId)
	p.sendUnmatchedRequest(&cancelReq)

	histDataWg.Wait()

	// Fill in any items that we didn't get from streaming data.
	for i, data := range histDataItems {
		if data == nil {
			continue
		}

		items := data.Data
		if len(items) == 0 {
			continue
		}

		item := &items[0]

		if item.Close >= 99999 {
			continue
		}

		switch equityHistDataTypes[i] {
		case ib.HistAsk:
			if output.Ask <= 0 || output.Ask >= 99999 {
				output.Ask = item.Close
			}
		case ib.HistBid:
			if output.Bid <= 0 || output.Bid >= 99999 {
				output.Bid = item.Close
			}
		case ib.HistTrades:
			if output.Last <= 0 || output.Last >= 99999 {
				output.Last = item.Close
			}

		case ib.HistVolatility:
			if output.OptionHistoricalVolatility <= 0 || output.OptionHistoricalVolatility >= 99999 {
				output.OptionHistoricalVolatility = item.Close
			}

		case ib.HistOptionIV:
			if output.OptionImpliedVolatility <= 0 || output.OptionImpliedVolatility >= 99999 {
				output.OptionImpliedVolatility = item.Close
			}
		}

	}

	if err == context.DeadlineExceeded {
		// Ignore the deadline exceeded error, and just return whatever we have but mark
		// the quote as incomplete.
		output.Incomplete = true
		err = nil
	}
	return output, err
}
