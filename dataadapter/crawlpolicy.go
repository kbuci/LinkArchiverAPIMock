package dataadapter

import (
	"errors"
	"math"
	"net/http"

	"github.com/temoto/robotstxt"
)

func requestCrawlPolicy(path string, host string) (int64, error) {
	archiverClient := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}}
	robotsURL := host + "/robots.txt"

	resp, err := archiverClient.Get(robotsURL)
	if err != nil {
		return 0, err
	}

	robotsData, err := robotstxt.FromResponse(resp)
	if err != nil {
		return 0, err
	}
	group := robotsData.FindGroup("*")
	if !group.Test(path) {
		return 0, errors.New("cannot crawl")
	}

	return int64(math.Round(group.CrawlDelay.Seconds())), nil
}
