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

func upload_message(lines_chan chan string, done_chan chan bool) {
	for {
		line, ok := <-lines_chan
		if !ok {
			fmt.Println("message chan closed")
			done_chan <- true
			return
		}
		fmt.Print("message: ")
		fmt.Println(parse_line(line))
	}
}

func process_one_file(filepath string, lines_chan chan string) {
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
		lines_chan <- scanner.Text()
		break
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func main() {
	// mc_connections =
	done_chan := make(chan bool)
	lines_chan := make(chan string)
	for i := 0; i < 4; i++ {
	}
	files, err := filepath.Glob("../data/*.gz")
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		go upload_message(lines_chan, done_chan)
	}
	for _, filepath := range files {
		process_one_file(filepath, lines_chan)
	}
	close(lines_chan)
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
