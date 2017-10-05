package brokerage_server_ib

import (
	"context"

	"github.com/dimfeld/brokerage_server/types"
	"github.com/dimfeld/ib"
)

func (p *IB) startAccountData() {
	if len(p.Accounts) == 0 {
		p.Logger.Error("No accounts found. Account and position updates will not function properly")
		return
	}

	// For now, always do the first account.
	request := ib.RequestAccountUpdates{
		Subscribe:   true,
		AccountCode: p.Accounts[0],
	}

	if err := p.sendUnmatchedRequest(&request); err != nil {
		p.Logger.Error("Error requesting account data", "err", err.Error())
	}
}

func (p *IB) handleAccountValue(v *ib.AccountValue) {
	accountId := v.Key.AccountCode
	p.LogDebugVerbose("Received account update", "data", v)

	p.accountDataMutex.Lock()
	account, ok := p.accountData[accountId]
	if !ok {
		account.ID = accountId
		account.Broker = BrokerName
	}
	switch v.Key.Key {
	case "AccountType":
		account.Type = v.Value
	case "NetLiquidation":
		account.NetLiquidation = v.Value
	case "BuyingPower":
		account.BuyingPower = v.Value
	case "InitMarginReq":
		account.MarginReq = v.Value
	case "AvailableFunds":
		account.AvailableFunds = v.Value
	case "RealizedPnL":
		account.RealizedPnL = v.Value
	case "UnrealizedPnL":
		account.UnrealizedPnL = v.Value
	}

	p.accountData[accountId] = account
	p.accountDataMutex.Unlock()
}

func (p *IB) handlePortfolioValue(v *ib.PortfolioValue) {
	var optionType types.PutOrCall
	if v.Contract.SecurityType == "OPT" {
		if v.Contract.Right == "P" || v.Contract.Right == "PUT" {
			optionType = types.Put
		} else {
			optionType = types.Call
		}

	}
	position := &types.Position{
		Symbol:     v.Contract.Symbol,
		Strike:     v.Contract.Strike,
		Multiplier: v.Contract.Multiplier,
		Expiration: v.Contract.Expiry,
		PutOrCall:  optionType,

		Position:      int(v.Position),
		MarketPrice:   v.MarketPrice,
		MarketValue:   v.MarketValue,
		AvgPrice:      v.AverageCost,
		UnrealizedPnL: v.UnrealizedPNL,
		RealizedPnL:   v.RealizedPNL,

		Broker:  "ib",
		Account: v.Key.AccountCode,

		RawData: v,
	}

	p.portfolioDataMutex.Lock()
	p.portfolioData[v.Key] = position
	p.portfolioDataMutex.Unlock()
}

func (p *IB) AccountList(ctx context.Context) ([]*types.Account, error) {
	p.accountDataMutex.Lock()
	accounts := make([]*types.Account, 0, len(p.accountData))
	for _, value := range p.accountData {
		accounts = append(accounts, &value)
	}
	p.accountDataMutex.Unlock()
	return accounts, nil
}

func (p *IB) GetPositions(ctx context.Context) ([]*types.Position, error) {
	p.portfolioDataMutex.RLock()
	result := make([]*types.Position, 0, len(p.portfolioData))
	for _, v := range p.portfolioData {
		result = append(result, v)
	}
	p.portfolioDataMutex.RUnlock()
	return result, nil
}
