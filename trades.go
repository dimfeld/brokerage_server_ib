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
	type tradeData struct {
		output types.Trade
		size   int
		price  float64
		side   string
	}

	req := &ib.RequestExecutions{}
	if !startTime.IsZero() {
		req.Filter = ib.ExecutionFilter{
			Time: startTime,
		}
	}

	trades := map[int64]*tradeData{}
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
				t = &tradeData{
					output: types.Trade{
						Symbol:  data.Contract.Symbol,
						Time:    data.Exec.Time,
						Account: data.Exec.AccountCode,
						Broker:  BrokerName,
						OrderId: strconv.FormatInt(data.Exec.PermID, 10),
					},
					size:  0,
					price: 0,
				}
				trades[data.Exec.PermID] = t
			}

			con := &data.Contract
			if con.SecurityType == "BAG" || con.SecurityType == "STK" {
				cq := int(data.Exec.CumQty)
				if cq > t.size {
					t.size = cq
					t.side = data.Exec.Side
				}

				t.price += data.Exec.Price * float64(data.Exec.Shares)
				if con.SecurityType == "BAG" {
					t.output.RawData = &data.Exec
					p.LogDebugNormal("bag trade", "data", data.Exec)
					break
				}
			}

			size := int(data.Exec.Shares)
			if data.Exec.Side == "SLD" {
				size *= -1
			}
			e := &types.Execution{
				ExecutionId: data.Exec.ExecID,
				Exchange:    con.Exchange,
				Size:        size,
				Price:       data.Exec.Price,
				Time:        data.Exec.Time,
			}
			if con.SecurityType == "OPT" {
				if con.Right == "P" || con.Right == "PUT" {
					e.OptionType = "PUT"
				} else {
					e.OptionType = "CALL"
				}
				e.Strike = con.Strike
				e.Expiration = ExpiryIBFormat(con.Expiry).toOCCFormat().String()
				if multiplier, err := strconv.Atoi(con.Multiplier); err == nil {
					e.Multiplier = multiplier
				}
			}

			e.RawData = data.Exec

			t.output.Executions = append(t.output.Executions, e)

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
		for _, e := range v.output.Executions {
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
		v.output.Price = v.price / float64(v.size)
		v.output.Size = v.size
		if v.side == "SLD" {
			v.output.Size *= -1
		}
		out = append(out, &v.output)
	}

	return out, nil
}
