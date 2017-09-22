package brokerage_server_ib

import (
	"context"
	"strconv"
	"time"

	"github.com/dimfeld/brokerage_server/types"
	log "github.com/inconshreveable/log15"
	"github.com/nothize/ib"
)

var logger = log.New("mod", "ib")

func (p *IB) GetStockQuote(ctx context.Context, symbol string) (*types.Quote, error) {
	const quoteFieldCount = 16

	req := &ib.RequestMarketData{
		Contract: ib.Contract{
			Symbol:       symbol,
			Currency:     "USD",
			SecurityType: "STK",
			Exchange:     "SMART",
		},
		Snapshot: true,
	}

	output := &types.Quote{
		Time: time.Now(),
	}
	seen := map[int64]bool{}

	err := p.syncMatchedRequest(ctx, req, func(r ib.Reply) (replyBehavior, error) {
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
			default:
				usedValue = false
			}

			if usedValue {
				seen[tick.Type] = true
			}

		case *ib.TickString:
			usedValue := true
			switch tick.Type {
			case ib.TickAskExch:
				output.AskExch = tick.Value
			case ib.TickBidExch:
				output.BidExch = tick.Value
			case ib.TickLastExchange:
				output.LastExch = tick.Value
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
			logger.Debug("quote exited early", "seen", seen)
			return REPLY_DONE, nil
		}

		if len(seen) == quoteFieldCount {
			return REPLY_DONE, nil
		}

		return REPLY_CONTINUE, nil
	})

	return output, err
}
