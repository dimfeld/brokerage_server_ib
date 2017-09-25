package brokerage_server_ib

import (
	"context"

	"github.com/dimfeld/brokerage_server/types"
	"github.com/dimfeld/ib"
)

func (p *IB) GetOptions(ctx context.Context, underlying string) (interface{}, error) {

	key := ContractKey{
		Symbol: underlying,
	}
	contractDetails, err := p.contractManager.GetContractDetails(ctx, key)
	if err != nil {
		return nil, err
	}

	p.LogDebugNormal("Got contract details", "data", contractDetails)

	contract := contractDetails[0].Summary
	request := &ib.RequestSecDefOptParams{
		Symbol:     contract.Symbol,
		SecType:    contract.SecurityType,
		ContractId: contract.ContractID,
		// No exchange, since the options trade on a different exchange than the underlying.
		Exchange: "",
	}

	var output []*ib.SecurityDefinitionOptionParameter
	err = p.syncMatchedRequest(ctx, request, func(r ib.Reply) (replyBehavior, error) {
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
