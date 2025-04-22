package utils

import (
	"fmt"
	"strings"
	"time"
)

func ParseTimeWithOffset(timeStr string, tzOffset string) *time.Time {
	if len(tzOffset) == 0 {
		tzOffset = "+00:00"
	}
	if strings.HasPrefix(tzOffset, "UTC") {
		tzOffset = strings.TrimPrefix(tzOffset, "UTC")
	}
	timeStr = strings.ReplaceAll(timeStr, " ", "")
	now := time.Now()
	tz, err := time.Parse("-07:00", tzOffset)
	if err != nil {
		return nil
	}
	ts, err := time.Parse(time.Kitchen, timeStr)
	if err != nil {
		return nil
	}
	date := time.Date(now.Year(), now.Month(), now.Day(), ts.Hour(), ts.Minute(), ts.Second(), ts.Nanosecond(), tz.Location())
	return &date
}

func NowWithOffset(tzOffset string) *time.Time {
	if strings.HasPrefix(tzOffset, "UTC") {
		tzOffset = strings.TrimPrefix(tzOffset, "UTC")
	}
	now := time.Now()
	tz, err := time.Parse("-07:00", tzOffset)
	if err != nil {
		return &now
	}
	inLoc := now.In(tz.Location())
	return &inLoc
}

func NextWeekDayAfter(weekday time.Weekday, t *time.Time) (*time.Time, error) {
	if t == nil {
		return nil, nil
	}
	diff := int(weekday) - int(t.Weekday())
	if diff <= 0 {
		diff += 7
	}
	date := t.AddDate(0, 0, diff)
	return &date, nil
}

func Pad(num int) string {
	if num < 10 {
		return fmt.Sprintf("0%d", num)
	}
	return fmt.Sprintf("%d", num)
}
