package brokerage_server_ib

import "errors"

type IB struct {
}

func (ib *IB) Connect(config map[string]interface{}) error {
	return errors.New("Not implemented")
}

func (ib *IB) Close() error {
	return nil
}

func New() *IB {
	return &IB{}
}
