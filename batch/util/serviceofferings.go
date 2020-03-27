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

	req := egoscale.ListServiceOfferings{}
	iResp, err := cl.Request(req)
	if err != nil {
		panic(err)
	}

	resp := iResp.(*egoscale.ListServiceOfferingsResponse)
	for _, x := range resp.ServiceOffering {
		log.Printf("%s (%s): %d CPU, %d Mem",
			x.ID.String(), x.Name, x.CPUNumber, x.Memory)
	}
}
