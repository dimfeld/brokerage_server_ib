package brokerage_server_ib

import (
	"context"
	"time"

	"github.com/dimfeld/brokerage_server/types"
	"github.com/dimfeld/ib"
)

func (p *IB) GetTrades(ctx context.Context, startTime time.Time) ([]*types.Trade, error) {
	req := &ib.RequestExecutions{}
	if !startTime.IsZero() {
		req.Filter = ib.ExecutionFilter{
			Time: startTime,
		}
	}

	executions := map[string]*types.Trade{}
	commissions := map[string]*ib.CommissionReport{}

	// Commissions reports come in as unmatched for some reason, so subscribe
	// to unmatched data to get them.
	unmChan, closeUnmData := p.subscribeUnmatched()
	go func() {
		for r := range unmChan {
			if data, ok := r.(*ib.CommissionReport); ok {
				commissions[data.ExecutionID] = data
			}
		}
	}()

	_, err := p.syncMatchedRequest(ctx, req, func(r ib.Reply) (replyBehavior, error) {
		switch data := r.(type) {
		case *ib.ExecutionData:
			e := executions[data.Exec.ExecID]
			if e == nil {
				e = &types.Trade{}
				executions[data.Exec.ExecID] = e
			}

			e.Account = data.Exec.AccountCode
			e.Broker = "ib"
			e.TradeId = data.Exec.ExecID
			con := &data.Contract
			e.Symbol = con.Symbol
			if con.SecurityType == "OPT" {
				if con.Right == "P" || con.Right == "PUT" {
					e.OptionType = "PUT"
				} else {
					e.OptionType = "CALL"
				}
				e.Strike = con.Strike
				e.Expiration = con.Expiry
				e.Multiplier = con.Multiplier
			}

			e.Exchange = con.Exchange
			e.Side = data.Exec.Side
			e.Size = int(data.Exec.Shares)
			e.Price = data.Exec.Price
			e.AvgPrice = data.Exec.AveragePrice
			e.CumQty = data.Exec.CumQty
			e.Time = data.Exec.Time
			e.RawData = data

		case *ib.CommissionReport:
			e := executions[data.ExecutionID]
			if e == nil {
				e = &types.Trade{}
				executions[data.ExecutionID] = e
			}

			e.Commissions = data.Commission
			e.RealizedPnL = data.RealizedPNL

		case *ib.ExecutionDataEnd:
			return REPLY_DONE, nil

		case *ib.ErrorMessage:
			return REPLY_DONE, data.Error()
		}
		return REPLY_CONTINUE, nil
	})

	closeUnmData()

	if err != nil {
		return nil, err
	}

	out := make([]*types.Trade, 0, len(executions))

	for _, v := range executions {
		comm := commissions[v.TradeId]
		if comm != nil {
			v.Commissions = comm.Commission
			if comm.RealizedPNL < 1e100 {
				v.RealizedPnL = comm.RealizedPNL
			}
		}
		out = append(out, v)
	}

	return out, nil
}
