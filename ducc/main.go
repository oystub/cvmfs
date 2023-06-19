package main

import (
	"fmt"

	"github.com/cvmfs/ducc/lib"
	"github.com/cvmfs/ducc/localdb"
	"github.com/cvmfs/ducc/rest"
	"github.com/cvmfs/ducc/scheduler"
)

func main() {

	fmt.Println("Initializing DUCC")
	lib.SetupNotification()
	lib.SetupRegistries()

	fmt.Println("Initializing local database")
	err := localdb.Init("./ducc.db")
	if err != nil {
		fmt.Println("Failed to initialize local database:", err)
		panic(err)
	}
	defer localdb.Close()

	fmt.Println("Starting REST API")
	go rest.Setup()

	fmt.Println("Starting scheduler")
	go scheduler.Main()

	//daemon.Main()

	//cmd.EntryPoint()
	select {}
}
