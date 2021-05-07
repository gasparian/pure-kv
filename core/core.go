package core

import (
	"errors"
	"io/ioutil"
	"os"
)

// Shortcuts for RPC methods
const (
	Size     = "PureKv.Size"
	Create   = "PureKv.Create"
	Destroy  = "PureKv.Destroy"
	Del      = "PureKv.Del"
	Set      = "PureKv.Set"
	Get      = "PureKv.Get"
	MakeIter = "PureKv.MakeIterator"
	Next     = "PureKv.Next"
	// Default files creation mode
	FileMode = 0700
)

// Record holds all needed data for each map entry
type Record struct {
	Key   string
	Value interface{}
}

// Response holds binary value from server and status
type Response struct {
	Record
	Ok bool
}

// Request holds keys and values, all optional
type Request struct {
	Record
	Bucket string
}

// CleanDir removes everythin from specified directory
func CleanDir(path string) error {
	err := os.RemoveAll(path)
	if err != nil {
		return err
	}
	err = os.MkdirAll(path, FileMode)
	if err != nil {
		return err
	}
	return nil
}

// CheckDirFiles checks if there are files in specified directory
func CheckDirFiles(path string) error {
	files, err := ioutil.ReadDir(path)
	if err != nil || len(files) == 0 {
		return errors.New("Can't find the db dump")
	}
	return nil
}
