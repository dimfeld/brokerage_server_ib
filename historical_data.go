package brokerage_server_ib

import (
	"context"
	"sync"
	"time"

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

			histDataItems[i], histDataErrors[i] = p.historicalData(ctx, &req)
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
