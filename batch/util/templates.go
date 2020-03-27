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

	req := egoscale.ListTemplates{
		TemplateFilter: "featured",
		Keyword:        "debian",
	}
	iResp, err := cl.Request(req)
	if err != nil {
		panic(err)
	}

	resp := iResp.(*egoscale.ListTemplatesResponse)
	for _, x := range resp.Template {
		log.Printf("%s (%s)", x.ID.String(), x.Name)
	}
}
