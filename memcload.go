package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	//"github.com/golang/protobuf/proto"
	//"github.com/bradfitz/gomemcache/memcache"
)

type ParsedLine struct {
	dev_type string
	dev_id   string
	lat      float64
	lon      float64
	apps     []int
}

func parse_line(line string) ParsedLine {
	var parsed ParsedLine
	var err error
	words := strings.Fields(line)
	parsed.dev_type = words[0]
	parsed.dev_id = words[1]
	parsed.lat, err = strconv.ParseFloat(words[2], 64)
	if err != nil {
		log.Fatal(err)
	}
	parsed.lon, err = strconv.ParseFloat(words[3], 64)
	if err != nil {
		log.Fatal(err)
	}

	raw_apps := strings.Split(words[4], ",")
	for _, s := range raw_apps {
		app, err := strconv.Atoi(s)
		if err != nil {
			log.Fatal(err)
		}
		parsed.apps = append(parsed.apps, app)
	}
	return parsed
}

func upload_message(data_chan chan ParsedLine, done_chan chan bool) {
	for {
		parsed_line, ok := <-data_chan
		if !ok {
			fmt.Println("data chan closed")
			done_chan <- true
			return
		}
		fmt.Print("message: ")
		fmt.Println(parsed_line)
	}
}

func process_one_file(filepath string, data_chan chan ParsedLine) {
	f, err := os.Open(filepath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}
	defer gr.Close()

	scanner := bufio.NewScanner(gr)
	for scanner.Scan() {
		line := scanner.Text()
		data_chan <- parse_line(line)
		break
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func main() {
	// mc_connections =
	done_chan := make(chan bool)
	data_chan := make(chan ParsedLine)
	for i := 0; i < 4; i++ {
	}
	files, err := filepath.Glob("../data/*.gz")
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		go upload_message(data_chan, done_chan)
	}
	for _, filepath := range files {
		process_one_file(filepath, data_chan)
	}
	close(data_chan)
	for i := 0; i < 4; i++ {
		<-done_chan
	}

	// channel with files
	// goroutine process one file
	// read gz line by line
	// parse
	// make  protobuf struct
	// send to messages queue
	// gouroutine uploader
}
