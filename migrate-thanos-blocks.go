package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/oklog/ulid"
	"github.com/prometheus/tsdb"
)

const (
	RFC3339NanoWithTrailingZeros = "2006-01-02T15:04:05.000000000Z07:00"
)

func main() {
	// Get the path
	path := os.Args[1]

	// List directory
	entries, err := ioutil.ReadDir(path)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		id, err := ulid.Parse(entry.Name())
		if err != nil {
			continue
		}

		// Read the metadata file
		metaFilepath := path + "/" + id.String() + "/meta.json"
		metaContent, err := ioutil.ReadFile(metaFilepath)
		if err != nil {
			fmt.Println(fmt.Sprintf("unable to read metadata file at %s: %s", metaFilepath, err.Error()))
			os.Exit(1)
		}

		// Parse the metadata
		metadata := tsdb.BlockMeta{}
		err = json.Unmarshal(metaContent, &metadata)
		if err != nil {
			fmt.Println(fmt.Sprintf("unable to parse metadata file at %s: %s", metaFilepath, err.Error()))
			os.Exit(1)
		}

		// Build the new name
		mint := time.Unix(0, metadata.MinTime*1000000).UTC().Format(RFC3339NanoWithTrailingZeros)
		maxt := time.Unix(0, metadata.MaxTime*1000000).UTC().Format(RFC3339NanoWithTrailingZeros)
		newName := fmt.Sprintf("mint-%s-maxt-%s-%s", mint, maxt, id.String())

		// Create the new directory
		err = os.Mkdir(path+"/"+newName, os.ModePerm)
		if err != nil && !os.IsExist(err) {
			fmt.Println(fmt.Sprintf("unable to create directory %s: %s", path+"/"+newName, err.Error()))
			os.Exit(1)
		}

		// Copy blocks from old directory to new directory
		oldDir := path + "/" + entry.Name() + "/"
		err = filepath.Walk(oldDir, func(entry string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			// Get the sub path
			subpath := entry[len(oldDir):]
			if subpath == "" {
				return nil
			}

			// Handle the case it's a directory
			if info.IsDir() {
				err := os.Mkdir(path+"/"+newName+"/"+subpath, os.ModePerm)
				if err != nil && !os.IsExist(err) {
					return err
				}

				return nil
			}

			// Handle the case it's a file. This way of copying file is very inefficient, but it's just
			// an hack to get something working quickly.
			fileContent, err := ioutil.ReadFile(entry)
			if err != nil {
				return err
			}

			return ioutil.WriteFile(path+"/"+newName+"/"+subpath, fileContent, os.ModePerm)
		})

		if err != nil {
			fmt.Println(fmt.Sprintf("error while copying files: %s", err.Error()))
			os.Exit(1)
		}
	}

}
