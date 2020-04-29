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
	sshKeyPath string
	lock       sync.Mutex
	vms        map[string]struct{} // Map of known VMs by ID (exoscale).
	launching  int                 // Number of VMs currently launching.

	collectedAt time.Time
	queue       iQueue
	cl          *egoscale.Client
}

func runVMSupervisor(queue iQueue, apiKey, apiSecret, sshKeyPath string) {
	vms := &vmSupervisor{
		sshKeyPath: sshKeyPath,
		cl: egoscale.NewClient(
			"https://api.exoscale.ch/compute",
			apiKey,
			apiSecret),
		vms:   map[string]struct{}{},
		queue: queue,
	}
	go vms.runCollector()
	for i := 0; i < 16; i++ {
		go vms.runLauncher()
	}
}

// ----------------------------------------------------------------------------

func (vms *vmSupervisor) log(s string, args ...interface{}) {
	log.Printf("[VM Supervisor] "+s, args...)
}

// ----------------------------------------------------------------------------

func (vms *vmSupervisor) putVM(vm egoscale.VirtualMachine) {
	id := vm.ID.String()

	vms.lock.Lock()
	defer vms.lock.Unlock()

	_, ok := vms.vms[id]
	if !ok {
		vms.log("Adding vm %s/%s...", id, vm.DefaultNic().IPAddress.String())
		vms.vms[id] = struct{}{}
		runVM(
			vms,
			vm.ID.String(),
			vm.DefaultNic().IPAddress.String(),
			"batch",
			vms.queue,
			vms.sshKeyPath)
	}
}

func (vms *vmSupervisor) delVM(id string) {
	vms.lock.Lock()
	defer vms.lock.Unlock()
	delete(vms.vms, id)
}

// Return true if the launching thread should lanuch a new VM. If the return
// value is true, the caller must then call `launchComplete` after the launch
// attempt is complete - regardless of the success.
func (vms *vmSupervisor) getLaunchAuthorization() bool {
	vms.lock.Lock()
	defer vms.lock.Unlock()

	if len(vms.vms)+vms.launching >= maxVMs {
		return false
	}

	wanted := vms.queue.Length() - len(vms.vms) - vms.launching
	if wanted <= 0 {
		return false
	}

	vms.launching++
	return true
}

func (vms *vmSupervisor) launchComplete() {
	vms.lock.Lock()
	defer vms.lock.Unlock()
	vms.launching--
}

// ----------------------------------------------------------------------------

func (vms *vmSupervisor) runCollector() {
	for {
		vms.collectVMs()
		time.Sleep(2 * time.Minute)
	}
}

func (vms *vmSupervisor) collectVMs() {
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
		if vm.DefaultNic() != nil && vm.DefaultNic().IPAddress != nil {
			vms.putVM(vm)
		}
	}
}

// ----------------------------------------------------------------------------

func (vms *vmSupervisor) runLauncher() {
	vms.log("Running launcher...")
	for {
		spawned, err := vms.attemptLaunch()
		if err != nil {
			time.Sleep(30 * time.Second)
			continue
		}
		if !spawned {
			time.Sleep(time.Second)
		}
	}
}

func (vms *vmSupervisor) attemptLaunch() (bool, error) {
	if !vms.getLaunchAuthorization() {
		return false, nil
	}
	defer vms.launchComplete()

	vms.log("Launching VM...")

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

	vms.delVM(id)
	return nil
}
