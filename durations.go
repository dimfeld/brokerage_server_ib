package brokerage_server_ib

import (
	"fmt"
	"math"
	"time"

	"github.com/dimfeld/ib"
)

func durationToBarWidth(t time.Duration) ib.HistDataBarSize {
	// This would be better as a binary search.
	if t >= time.Hour*24*30 {
		return ib.HistBarSize1Month
	} else if t >= time.Hour*24*7 {
		return ib.HistBarSize1Week
	} else if t >= time.Hour*24 {
		return ib.HistBarSize1Day
	} else if t >= time.Hour*8 {
		return ib.HistBarSize8Hour
	} else if t >= time.Hour*4 {
		return ib.HistBarSize4Hour
	} else if t >= time.Hour*2 {
		return ib.HistBarSize2Hour
	} else if t >= time.Hour*1 {
		return ib.HistBarSize1Hour
	} else if t >= time.Minute*30 {
		return ib.HistBarSize30Min
	} else if t >= time.Minute*20 {
		return ib.HistBarSize20Min
	} else if t >= time.Minute*15 {
		return ib.HistBarSize15Min
	} else if t >= time.Minute*10 {
		return ib.HistBarSize10Min
	} else if t >= time.Minute*5 {
		return ib.HistBarSize5Min
	} else if t >= time.Minute*3 {
		return ib.HistBarSize3Min
	} else if t >= time.Minute*2 {
		return ib.HistBarSize2Min
	} else if t >= time.Minute*1 {
		return ib.HistBarSize1Min
	} else if t >= time.Second*30 {
		return ib.HistBarSize30Sec
	} else if t >= time.Second*15 {
		return ib.HistBarSize15Sec
	} else if t >= time.Second*10 {
		return ib.HistBarSize10Sec
	} else if t >= time.Second*5 {
		return ib.HistBarSize5Sec
	} else {
		return ib.HistBarSize1Sec
	}

}

func durationToIbDuration(t time.Duration) string {
	if t >= time.Hour*24 {
		return fmt.Sprintf("%d D", int(math.Ceil(float64(t)/float64(time.Hour*24))))
	} else {
		return fmt.Sprintf("%d S", int(math.Ceil(float64(t)/float64(time.Second))))
	}
}
