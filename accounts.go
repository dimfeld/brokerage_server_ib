package brokerage_server_ib

import (
	"context"
	"errors"

	"github.com/dimfeld/brokerage_server/types"
	"github.com/dimfeld/ib"
)

func (p *IB) accountDataLoop() error {
	request := ib.RequestAccountSummary{
		Group: "All",
		Tags:  "AccountType,NetLiquidation,BuyingPower,InitMarginReq,AvailableFunds",
	}

	_, datachan, err := p.startStreamingRequest(context.Background(), &request)
	if err != nil {
		return errors.New("Failed to make account data connection")
	}

	go func() {
		for r := range datachan {
			switch data := r.(type) {
			case *ib.AccountSummary:
				accountId := data.Key.AccountCode
				p.LogDebugVerbose("Received account update", "data", data)

				p.accountDataMutex.Lock()
				account, ok := p.accountData[accountId]
				if !ok {
					account.ID = accountId
					account.Broker = BrokerName
				}
				switch data.Key.Key {
				case "AccountType":
					account.Type = data.Value
				case "NetLiquidation":
					account.NetLiquidation = data.Value
				case "BuyingPower":
					account.BuyingPower = data.Value
				case "InitMarginReq":
					account.MarginReq = data.Value
				case "AvailableFunds":
					account.AvailableFunds = data.Value
				default:
					p.Logger.Warn("Received unexpected account update", "key", data.Key.Key, "value", data.Value)
				}
				p.accountData[accountId] = account
				p.accountDataMutex.Unlock()
			case *ib.ErrorMessage:
				p.Logger.Error("account data connection error", "err", data.Error())
			}
		}
	}()

	return nil
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
