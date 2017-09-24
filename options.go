package brokerage_server_ib

import (
	"context"

	"github.com/dimfeld/brokerage_server/types"
	"github.com/dimfeld/ib"
)

func (p *IB) GetOptions(ctx context.Context, underlying string) (interface{}, error) {

	request := &ib.RequestSecDefOptParams{
		Symbol:     underlying,
		SecType:    "STK",
		Exchange:   "SMART",
		ContractId: 416904,
	}

	var output []*ib.SecurityDefinitionOptionParameter
	err := p.syncMatchedRequest(ctx, request, func(r ib.Reply) (replyBehavior, error) {
		switch data := r.(type) {
		case *ib.SecurityDefinitionOptionParameter:
			output = append(output, data)
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

	return output, err
}
