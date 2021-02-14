package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"log"
	"os"
	"path/filepath"
	//"google.golang.org/protobuf"
	//"github.com/bradfitz/gomemcache/memcache"
)

func process_one_file(files_chan chan string, done_chan chan bool) {
	for {
		filepath, ok := <-files_chan
		fmt.Println("in goroutine: " + filepath)
		if !ok {
			fmt.Println("closed")
			done_chan <- true
			return
		}
		fmt.Printf("in goroutine %s\n", filepath)

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

		fmt.Println("hey")
		scanner := bufio.NewScanner(gr)
		fmt.Println(scanner)
		//line := 1
		for scanner.Scan() {
			fmt.Println(scanner.Text())
		}

		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}

	}
}

func main() {
	// mc_connections =
	files_chan := make(chan string)
	done_chan := make(chan bool)
	for i := 0; i < 4; i++ {
		go process_one_file(files_chan, done_chan)
	}
	files, err := filepath.Glob("../data/*.gz")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(files)
	for _, filepath := range files {
		files_chan <- filepath
	}
	close(files_chan)
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
