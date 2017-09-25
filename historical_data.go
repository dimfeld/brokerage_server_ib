package brokerage_server_ib

import (
	"context"

	"github.com/dimfeld/ib"
)

func (p *IB) historicalData(ctx context.Context, req *ib.RequestHistoricalData) (*ib.HistoricalData, error) {

	var output *ib.HistoricalData
	_, err := p.syncMatchedRequest(ctx, req, func(r ib.Reply) (replyBehavior, error) {
		switch data := r.(type) {
		case *ib.ErrorMessage:
			return REPLY_DONE, data.Error()
		case *ib.HistoricalData:
			output = data
			return REPLY_DONE, nil
		default:
			p.Logger.Warn("HistoricalData: unexpected reply", "msg", r)
		}

		return REPLY_CONTINUE, nil
	})

	return output, err
}

// func (p *IB) GetHistoricalData(ctx context.Context, params types.HistoricalDataParams) ()
