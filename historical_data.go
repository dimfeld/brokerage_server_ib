package brokerage_server_ib

import (
	"context"
	"sync"
	"time"

	"github.com/dimfeld/brokerage_server/types"
	"github.com/dimfeld/ib"
)

func (p *IB) getLatestHistData(ctx context.Context, contract *ib.Contract, histTypes []ib.HistDataToShow) ([]*ib.HistoricalData, error) {

	wg := sync.WaitGroup{}
	histDataItems := make([]*ib.HistoricalData, len(histTypes))
	histDataErrors := make([]error, len(histTypes))
	wg.Add(len(histTypes))

	// Get a single bar for each historical data item. We do this because the IB
	// streaming data isn't always reliable, especially after hours.
	now := time.Now()
	for i, histType := range histTypes {
		go func(i int, histType ib.HistDataToShow) {
			defer wg.Done()
			req := ib.RequestHistoricalData{
				Contract:    *contract,
				EndDateTime: now,
				Duration:    "60 S",
				// Use 1 minute bars to avoid hitting IB's pacing restrictions.
				BarSize:    ib.HistBarSize1Min,
				WhatToShow: histType,
				UseRTH:     true,
			}

			histDataItems[i], histDataErrors[i] = p.oneHistoricalData(ctx, &req)
		}(i, histType)
	}

	wg.Wait()
	var err error
	for i, e := range histDataErrors {
		if e != nil {
			p.Logger.Error("Historical data error",
				"type", histTypes[i],
				"error", e.Error())
			err = e
		}
	}

	return histDataItems, err
}

func (p *IB) oneHistoricalData(ctx context.Context, req *ib.RequestHistoricalData) (*ib.HistoricalData, error) {

	var output *ib.HistoricalData
	_, err := p.syncMatchedRequest(ctx, req, func(r ib.Reply) (replyBehavior, error) {
		switch data := r.(type) {
		case *ib.ErrorMessage:
			if data.Code == IBErrNoDataReturned {
				return REPLY_DONE, nil
			}
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

func (p *IB) GetHistoricalData(ctx context.Context, params types.HistoricalDataParams) ([]*types.Quote, error) {

	which := params.Which
	var dataType ib.HistDataToShow
	switch params.Which {
	default:
		which = types.HistoricalDataTypePrice
		fallthrough
	case types.HistoricalDataTypePrice:
		dataType = ib.HistTrades
	case types.HistoricalDataTypeIv:
		dataType = ib.HistOptionIV
	case types.HistoricalDataTypeHv:
		dataType = ib.HistVolatility

	}

	if params.Option.Underlying != "" && dataType != ib.HistTrades {
		return nil, types.ArgError("options historical quotes must be for price data")
	}

	var key ContractKey
	if params.Symbol != "" {
		key = ContractKey{
			Symbol:       params.Symbol,
			SecurityType: "STK",
		}
	} else {
		key = ContractKeyFromOption(&params.Option)
	}
	details, err := p.contractManager.GetContractDetails(ctx, key)
	if err != nil {
		return nil, err
	} else if len(details) == 0 {
		return nil, types.ArgError("Contract not found")
	}

	// Get an estimate of the proper array size.
	totalBars := params.Duration / params.BarWidth
	output := make([]*types.Quote, 0, totalBars)

	req := &ib.RequestHistoricalData{
		Contract:    details[0].Summary,
		EndDateTime: params.EndTime,
		WhatToShow:  dataType,
		Duration:    durationToIbDuration(params.Duration),
		BarSize:     durationToBarWidth(params.BarWidth),
		UseRTH:      !params.IncludeAH,
	}

	_, err = p.syncMatchedRequest(ctx, req, func(r ib.Reply) (replyBehavior, error) {
		switch data := r.(type) {
		case *ib.ErrorMessage:
			return REPLY_DONE, data.Error()
		case *ib.HistoricalData:
			for i, _ := range data.Data {
				item := &data.Data[i]
				quote := &types.Quote{
					High:   item.High,
					Low:    item.Low,
					Open:   item.Open,
					Close:  item.Close,
					Volume: item.Volume,
					Time:   item.Date,
				}

				if which == types.HistoricalDataTypeHv {
					quote.OptionHistoricalVolatility = item.Close
				} else if which == types.HistoricalDataTypeIv {
					quote.OptionImpliedVolatility = item.Close
				}
				output = append(output, quote)
			}

			return REPLY_DONE, nil
		default:
			p.Logger.Warn("HistoricalData: unexpected reply", "msg", r)
		}

		return REPLY_CONTINUE, nil
	})

	return output, nil
}
