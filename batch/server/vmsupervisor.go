package server

import (
	"encoding/base64"
	"log"
	"sync"
	"time"

	"github.com/exoscale/egoscale"
)

const (
	maxVMs     = 200
	esDiskSize = 50                                     // In GB
	esZone     = "35eb7739-d19e-45f7-a581-4687c54d6d02" // de-fra-1
	//esServiceOffering = "b6e9d1e8-89fc-4db3-aaa4-9b4c5b1d0844" // 2 core/4GB
	esServiceOffering = "c6f99499-7f59-4138-9427-a09db13af2bc" // 4 core/8GB
	esTemplate        = "b7ce66da-8bd1-4a02-8788-0bd880038b01" // Debian 10
	esKeyPair         = "data@popcoin"
)

const vmInitScript = `#!/bin/bash
echo "* soft nofile 1000000" >> /etc/security/limits.conf
echo "* hard nofile 1000000" >> /etc/security/limits.conf
adduser -disabled-password --gecos "" batch
cp -r /root/.ssh /home/batch/
chown -R batch:batch /home/batch/.ssh
touch /batch.ready
`

type vmSupervisor struct {
	sshKeyPath  string
	lock        sync.Mutex
	vms         map[string]struct{} // Map of known VMs by ID.
	collectedAt time.Time
	queue       iQueue
	cl          *egoscale.Client
}

func runVMSupervisor(queue iQueue, apiKey, apiSecret, sshKeyPath string) {
	cl := &vmSupervisor{
		sshKeyPath: sshKeyPath,
		cl: egoscale.NewClient(
			"https://api.exoscale.ch/compute",
			apiKey,
			apiSecret),
		vms:   map[string]struct{}{},
		queue: queue,
	}
	go cl.run()
}

// ----------------------------------------------------------------------------

func (vms *vmSupervisor) log(s string, args ...interface{}) {
	log.Printf("[VM Supervisor] "+s, args...)
}

// ----------------------------------------------------------------------------

func (vms *vmSupervisor) getVMCount() int {
	vms.lock.Lock()
	defer vms.lock.Unlock()
	return len(vms.vms)
}

func (vms *vmSupervisor) putVM(vm egoscale.VirtualMachine) {
	id := vm.ID.String()

	vms.lock.Lock()
	defer vms.lock.Unlock()

	_, ok := vms.vms[id]
	if !ok {
		vms.log("Adding vm %s...", id)
		vms.vms[id] = struct{}{}
		runVM(vm, vms, vms.queue, vms.sshKeyPath)
	}
}

func (vms *vmSupervisor) delVM(id string) {
	vms.lock.Lock()
	defer vms.lock.Unlock()
	delete(vms.vms, id)
}

// ----------------------------------------------------------------------------

func (vms *vmSupervisor) run() {
	var next stateFunc = vms.collectVMs

	for next != nil {
		next = next()
	}
}

// ----------------------------------------------------------------------------

// sleep -> (collectVMs, scaleUp)
func (vms *vmSupervisor) sleep() stateFunc {
	time.Sleep(time.Second)
	if time.Since(vms.collectedAt) > 2*time.Minute {
		return vms.collectVMs
	}
	return vms.scaleUp
}

func (vms *vmSupervisor) sleepError() stateFunc {
	time.Sleep(29 * time.Second)
	return vms.sleep
}

// collectVMs -> (scaleUp, sleepError)
func (vms *vmSupervisor) collectVMs() stateFunc {
	vms.collectedAt = time.Now()

	vms.log("Getting list of running vms...")
	req := &egoscale.ListVirtualMachines{}

	iResp, err := vms.cl.Request(req)
	if err != nil {
		vms.log("Failed to get list of running vms: %v", err)
		return vms.sleepError
	}

	resp := iResp.(*egoscale.ListVirtualMachinesResponse)
	vms.log("Found %d running vms.", len(resp.VirtualMachine))
	for _, vm := range resp.VirtualMachine {
		vms.putVM(vm)
	}

	return vms.scaleUp
}

// scaleUp -> (scaleUp, sleepError, sleep)
func (vms *vmSupervisor) scaleUp() stateFunc {
	queueLen := vms.queue.Length()
	numVMs := vms.getVMCount()

	if numVMs >= maxVMs || queueLen <= 2*numVMs {
		return vms.sleep
	}

	vms.log("Scaling up...")

	req := &egoscale.DeployVirtualMachine{
		Size:              esDiskSize,
		ServiceOfferingID: egoscale.MustParseUUID(esServiceOffering),
		TemplateID:        egoscale.MustParseUUID(esTemplate),
		ZoneID:            egoscale.MustParseUUID(esZone),
		KeyPair:           esKeyPair,
		UserData:          base64.StdEncoding.EncodeToString([]byte(vmInitScript)),
	}

	resp, err := vms.cl.Request(req)
	if err != nil {
		vms.log("Failed to deploy vm: %v", err)
		return vms.sleepError
	}

	vms.putVM(*resp.(*egoscale.VirtualMachine))

	return vms.scaleUp
}

// ----------------------------------------------------------------------------

func (vms *vmSupervisor) DeleteVM(id string) error {
	vms.log("Deleting vm %s...", id)

	req := &egoscale.DestroyVirtualMachine{
		ID: egoscale.MustParseUUID(id),
	}
	_, err := vms.cl.Request(req)
	if err != nil {
		if resp, ok := err.(*egoscale.ErrorResponse); ok {
			if resp.ErrorCode == egoscale.ParamError {
				err = nil
			}
		}
	}

	if err != nil {
		vms.log("Failed to delete vm %s: %v", id, err)
		return err
	}

	// TODO: Wait for delete to take affect?

	vms.delVM(id)
	return nil
}
