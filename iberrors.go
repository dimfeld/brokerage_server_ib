package brokerage_server_ib

type IBError int

const (
	IBErrMarketDataConnectionOk     IBError = 2104
	IBErrHistoricalDataConnectionOk         = 2106
)
