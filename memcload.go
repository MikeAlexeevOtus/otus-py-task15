package main

import (
    "fmt"
    "path/filepath"

    //"google.golang.org/protobuf"
    //"github.com/bradfitz/gomemcache/memcache"
)

func process_one_file(files_chan chan string, done_chan chan bool) {
    for {
        filepath, ok := <- files_chan
        if !ok {
            fmt.Println("closed")
            done_chan <- true
            return
        }
        fmt.Printf("in goroutine %s\n", filepath)
    }
}

func main() {
    // mc_connections = 
    files_chan := make(chan string)
    done_chan := make(chan bool)
    for i := 0; i<4; i++ {
        go process_one_file(files_chan, done_chan)
    }
    files, err := filepath.Glob("../data/*.gz")
    if err != nil {
        fmt.Println("files not found")
        return
    }
    fmt.Println(files)
    for _, filepath := range files{
        fmt.Println(filepath)
        files_chan <- filepath
    }
    close(files_chan)
    <-done_chan


    // channel with files
    // goroutine process one file
    // read gz line by line
    // parse
    // make  protobuf struct
    // send to messages queue
    // gouroutine uploader
}
