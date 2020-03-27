package server

import (
	"fmt"
	"log"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/Suburbia-io/cloud/batch"
	"github.com/exoscale/egoscale"
)

type vm struct {
	supervisor iVMSupervisor
	id         string
	addr       string
	user       string
	executable string // Previous executable.
	queue      iQueue
	task       task
	sshKeyPath string
}

func runVM(
	eVM egoscale.VirtualMachine,
	supervisor iVMSupervisor,
	queue iQueue,
	sshKeyPath string,
) {
	vm := &vm{
		supervisor: supervisor,
		id:         eVM.ID.String(),
		addr:       eVM.DefaultNic().IPAddress.String(),
		user:       "batch",
		queue:      queue,
		sshKeyPath: sshKeyPath,
	}
	go vm.run()
}

// ----------------------------------------------------------------------------

func (vm *vm) log(s string, args ...interface{}) {
	log.Printf("["+vm.addr+"] "+s, args...)
}

// ----------------------------------------------------------------------------

func (vm *vm) run() {
	var next stateFunc = vm.start

	for next != nil {
		next = next()
	}
}

// start -> (getNextTask, stop)
func (vm *vm) start() stateFunc {
	tStart := time.Now()
	for time.Since(tStart) < 10*time.Minute {
		vm.log("Checking if server is running...")
		if vm.sshRun("stat /batch.ready") == nil {
			return vm.getNextTask
		}
		time.Sleep(time.Second)
	}
	return vm.stop
}

// getNextTask -> (runTask, stop)
func (vm *vm) getNextTask() stateFunc {
	vm.log("Waiting for next task...")
	task, ok := vm.queue.Next(2 * time.Minute)
	if !ok {
		return vm.stop
	}
	vm.task = task
	return vm.runTask
}

// runTask -> (getNextTask, stop)
func (vm *vm) runTask() stateFunc {
	var (
		err       error
		task      = vm.task
		result    batch.Result
		rTaskDir  = filepath.Join("/home", vm.user, "batch")
		rExecPath = filepath.Join("/home", vm.user, "batch", "exe")
	)

	result.Code = -1
	defer func() {
		task.Result <- result
	}()

	if vm.executable != task.Executable {

		vm.log("Cleaning up remote dir...")

		err = vm.sshRun("rm -rf %s && mkdir -p %s", rTaskDir, rTaskDir)
		if err != nil {
			msg := fmt.Sprintf("Failed to clean up remote task directory: %v", err)
			result.Output = msg
			vm.log(msg)
			return vm.stop
		}

		vm.log("Uploading executable...")

		if err = vm.scp(task.Executable, rExecPath); err != nil {
			msg := fmt.Sprintf("Failed to upload new executable: %v", err)
			result.Output = msg
			vm.log(msg)
			return vm.stop
		}
	}

	// Build run script.
	b := strings.Builder{}
	for key, value := range task.Env {
		b.WriteString("export ")
		b.WriteString(key)
		b.WriteString("=")
		b.WriteString(value)
		b.WriteString("\n")
	}

	b.WriteString(task.Executable)
	b.WriteString(" ")
	b.WriteString(task.Args)

	vm.log("Executing remote task...")

	cmd := vm.sshCmd(b.String())

	buf, err := cmd.CombinedOutput()

	result.Output = string(buf)
	result.Code = cmd.ProcessState.ExitCode()

	if err != nil {
		// If the command ran but failed, that's OK.
		if _, ok := err.(*exec.ExitError); !ok {
			vm.log("Failed to run remote command: %v", err)
			return vm.stop
		}
	}

	return vm.getNextTask
}

// stop -> nil
func (vm *vm) stop() stateFunc {
	for {
		vm.log("Deleting vm...")
		err := vm.supervisor.DeleteVM(vm.id)
		if err == nil {
			vm.log("Deleted")
			return nil
		}

		vm.log("Failed to delete VM: %v", err)
		time.Sleep(30 * time.Second)
	}
}

// ----------------------------------------------------------------------------

func (vm *vm) sshCmd(remoteCmd string, args ...interface{}) *exec.Cmd {
	return exec.Command(
		"ssh",
		"-F", "/dev/null",
		"-i", vm.sshKeyPath,
		"-o", "BatchMode yes",
		"-o", "ControlMaster auto",
		"-o", "ControlPersist yes",
		"-o", "ControlPath /tmp/_batch-ssh-socket-%r@%h:%p",
		"-o", "StrictHostKeyChecking no",
		"-o", "UserKnownHostsFile /dev/null",
		"-o", "ConnectTimeout=10",
		vm.user+"@"+vm.addr,
		fmt.Sprintf(remoteCmd, args...))
}

func (vm *vm) sshRun(remoteCmd string, args ...interface{}) error {
	return vm.sshCmd(remoteCmd, args...).Run()
}

func (vm *vm) scp(lPath, rPath string) error {
	cmd := exec.Command(
		"scp",
		"-F", "/dev/null",
		"-i", vm.sshKeyPath,
		"-o", "BatchMode yes",
		"-o", "ControlMaster auto",
		"-o", "ControlPersist yes",
		"-o", "ControlPath /tmp/_batch-ssh-socket-%r@%h:%p",
		"-o", "StrictHostKeyChecking no",
		"-o", "UserKnownHostsFile /dev/null",
		"-o", "ConnectTimeout=10",
		lPath,
		vm.user+"@"+vm.addr+":"+rPath)
	return cmd.Run()
}
