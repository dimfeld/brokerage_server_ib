package brokerage_server_ib

import (
	"context"
	"math"
	"strconv"
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

	trades := map[int64]*types.Trade{}
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
			t := trades[data.Exec.PermID]
			if t == nil {
				t = &types.Trade{
					Symbol:  data.Contract.Symbol,
					Time:    data.Exec.Time,
					Account: data.Exec.AccountCode,
					Broker:  BrokerName,
					OrderId: strconv.FormatInt(data.Exec.PermID, 10),
					RawData: data.Contract,
				}
				trades[data.Exec.PermID] = t
			}

			con := &data.Contract
			if con.SecurityType == "BAG" {
				// The BAG container execution isn't relevant to anything we need
				// since the component legs come in separately.
				break
			}

			e := &types.Execution{
				ExecutionId: data.Exec.ExecID,
				Exchange:    con.Exchange,
				Side:        data.Exec.Side,
				Size:        int(data.Exec.Shares),
				Price:       data.Exec.Price,
				AvgPrice:    data.Exec.AveragePrice,
			}
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
			e.Time = data.Exec.Time
			e.RawData = data.Exec

			t.Executions = append(t.Executions, e)

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

	out := make([]*types.Trade, 0, len(trades))

	for _, v := range trades {
		// Fill in the commissions for each execution.
		for _, e := range v.Executions {
			comm := commissions[e.ExecutionId]
			if comm != nil {
				e.Commissions = comm.Commission
				if comm.RealizedPNL < 1e100 { // Filter out meaningless data.
					e.RealizedPnL = comm.RealizedPNL
				} else {
					e.RealizedPnL = math.NaN()
				}
			}
		}
		out = append(out, v)
	}

	return out, nil
}
