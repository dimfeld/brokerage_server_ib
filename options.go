package brokerage_server_ib

import (
	"context"
	"sort"

	"github.com/dimfeld/brokerage_server/types"
	"github.com/dimfeld/ib"
)

func (p *IB) GetOptionsChain(ctx context.Context, underlying string) (types.OptionChain, error) {

	key := ContractKey{
		Symbol: underlying,
	}
	contractDetails, err := p.contractManager.GetContractDetails(ctx, key)
	if err != nil {
		return types.OptionChain{}, err
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

	expirations := map[string]bool{}
	strikes := map[float64]bool{}
	exchanges := []string{}
	var multiplier string

	err = p.syncMatchedRequest(ctx, request, func(r ib.Reply) (replyBehavior, error) {
		switch data := r.(type) {
		case *ib.SecurityDefinitionOptionParameter:

			for _, value := range data.Expirations {
				expirations[value] = true
			}

			for _, value := range data.Strikes {
				strikes[value] = true
			}

			exchanges = append(exchanges, data.Exchange)
			multiplier = data.Multiplier // I think multiplier is always the same?
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

	strikesList := make([]float64, 0, len(strikes))
	for s := range strikes {
		strikesList = append(strikesList, s)
	}

	expiresList := make([]string, 0, len(expirations))
	for e := range expirations {
		expiresList = append(expiresList, e)
	}

	sort.Strings(expiresList)
	sort.Float64s(strikesList)

	output := types.OptionChain{
		Underlying:  underlying,
		Multiplier:  multiplier,
		Strikes:     strikesList,
		Expirations: expiresList,
	}

	return output, err
}
