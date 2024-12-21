package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"tatzelwurm/chunkserver"
	"tatzelwurm/mainserver"
)

func main() {
	current_working_directory, err := os.Getwd()
	if err != nil {
		fmt.Println("Error while getting the current working directory, more info", err.Error())
		os.Exit(1)
	}
	run_mode := flag.String("run_mode", "none", "The mode in which to run, possible values: mainserver, chunkserver")
	config_file_path := flag.String("config_path", filepath.Join(filepath.Dir(current_working_directory), "config/mainserver/main_server.json"), "The configuration file path to run the particular component")

	flag.Parse()
	fmt.Printf("Run Mode: %s\n", *run_mode)
	if *run_mode == "mainserver" {
		mainserver.Run(*config_file_path)
	} else if *run_mode == "chunkserver" {
		chunkserver.Run(*config_file_path)
	} else {
		fmt.Printf("Unidentified run_mode %s, quitting...\n", *run_mode)
	}

}
