package main

import (
	daemon "github.com/cvmfs/ducc/daemon"
	"github.com/cvmfs/ducc/lib"
)

func main() {
	lib.SetupNotification()
	lib.SetupRegistries()
	daemon.Main()

	//cmd.EntryPoint()
}
