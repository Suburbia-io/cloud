package main

import (
	"log"
	"os"

	"github.com/exoscale/egoscale"
)

var endpoint = "https://api.exoscale.ch/compute"

func main() {
	cl := egoscale.NewClient(
		endpoint,
		os.Getenv("SB_COMPUTE_API_KEY"),
		os.Getenv("SB_COMPUTE_API_SECRET"))

	req := egoscale.ListZones{}
	iResp, err := cl.Request(req)
	if err != nil {
		panic(err)
	}

	resp := iResp.(*egoscale.ListZonesResponse)
	for _, x := range resp.Zone {
		log.Printf("%s (%s): %s ", x.ID.String(), x.Name, x.DisplayText)
	}
}
