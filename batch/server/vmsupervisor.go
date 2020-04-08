package server

import (
	"encoding/base64"
	"log"
	"sync"
	"time"

	"github.com/exoscale/egoscale"
)

const (
	maxVMs            = 200
	esDiskSize        = 50                                     // In GB
	esZone            = "35eb7739-d19e-45f7-a581-4687c54d6d02" // de-fra-1
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
	go cl.runCollectVMs()
	for i := 0; i < 16; i++ {
		go cl.runScaleUp()
	}
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

func (vms *vmSupervisor) runCollectVMs() {
	for {
		vms.runCollectVMsInner()
		time.Sleep(2 * time.Minute)
	}
}

func (vms *vmSupervisor) runCollectVMsInner() {
	vms.collectedAt = time.Now()

	vms.log("Getting list of running vms...")
	req := &egoscale.ListVirtualMachines{}

	iResp, err := vms.cl.Request(req)
	if err != nil {
		vms.log("Failed to get list of running vms: %v", err)
		return
	}

	resp := iResp.(*egoscale.ListVirtualMachinesResponse)
	vms.log("Found %d running vms.", len(resp.VirtualMachine))
	for _, vm := range resp.VirtualMachine {
		vms.putVM(vm)
	}
}

// ----------------------------------------------------------------------------

func (vms *vmSupervisor) runScaleUp() {
	for {
		spawned, err := vms.runScaleUpInner()
		if err != nil {
			time.Sleep(30 * time.Second)
			continue
		}
		if !spawned {
			time.Sleep(time.Second)
		}
	}
}

func (vms *vmSupervisor) runScaleUpInner() (bool, error) {
	queueLen := vms.queue.Length()
	numVMs := vms.getVMCount()

	// How many to spawn?
	N := 0
	switch queueLen {
	case 0:
		// Do nothing.
	case 1:
		if numVMs == 0 {
			N = 1
		}
	default:
		N = queueLen/2 - numVMs + 1
	}

	if numVMs+N > maxVMs {
		N = maxVMs - numVMs
	}

	if N <= 0 {
		return false, nil
	}

	vms.log("Launching VMs...")

	req := &egoscale.DeployVirtualMachine{
		RootDiskSize:      esDiskSize,
		ServiceOfferingID: egoscale.MustParseUUID(esServiceOffering),
		TemplateID:        egoscale.MustParseUUID(esTemplate),
		ZoneID:            egoscale.MustParseUUID(esZone),
		KeyPair:           esKeyPair,
		UserData:          base64.StdEncoding.EncodeToString([]byte(vmInitScript)),
	}

	resp, err := vms.cl.Request(req)
	if err != nil {
		vms.log("Failed to deploy vm: %v", err)
		return false, err
	}

	vms.putVM(*resp.(*egoscale.VirtualMachine))
	return true, nil
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
