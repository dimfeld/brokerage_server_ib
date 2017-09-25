package brokerage_server_ib

import (
	"context"
	"strconv"
	"time"

	"github.com/dimfeld/brokerage_server/types"
	"github.com/dimfeld/ib"
)

func (p *IB) GetStockQuote(ctx context.Context, symbol string) (*types.Quote, error) {
	const quoteFieldCount = 12

	// We don't get the "end" message for 11 seconds which is way too long in the case
	// where IB doesn't give us all the data. 3 seconds is more than enough time to get
	// the data in my experience, while still seeming somewhat responsive.
	ctx, cancelFunc := context.WithTimeout(ctx, time.Duration(3)*time.Second)
	defer cancelFunc()

	key := ContractKey{Symbol: symbol}
	details, err := p.contractManager.GetContractDetails(ctx, key)
	if err != nil {
		return nil, err
	}

	contract := &details[0].Summary

	req := &ib.RequestMarketData{
		Contract: ib.Contract{
			Symbol:       contract.Symbol,
			Currency:     contract.Currency,
			SecurityType: contract.SecurityType,
			Exchange:     contract.Exchange,
		},
		Snapshot: true,
	}

	output := &types.Quote{
		Time: time.Now(),
	}
	seen := map[int64]bool{}

	err = p.syncMatchedRequest(ctx, req, func(r ib.Reply) (replyBehavior, error) {
		switch tick := r.(type) {
		// case *ib.TickGeneric:
		// 	usedValue := true
		// 	switch tick.Type {
		// 	default:
		// 		usedValue = false
		// 	}

		// 	if usedValue {
		// 		seen[tick.Type] = true
		// 	}

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
			// case ib.TickMarkPrice:
			// 	output.Mark = tick.Price
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
					output.LastTime = time.Unix(int64(t), 0)
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

	if err == context.DeadlineExceeded {
		// Ignore the deadline exceeded error, and just return whatever we have but mark
		// the quote as incomplete.
		output.Incomplete = true
		err = nil
	}
	return output, err
}
