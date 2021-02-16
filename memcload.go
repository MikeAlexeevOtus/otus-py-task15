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

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/golang/protobuf/proto"
)

const UPLOAD_WORKERS int = 4

type ParsedLine struct {
	dev_type string
	dev_id   string
	lat      float64
	lon      float64
	apps     []uint32
}

func make_protobuf_struct(parsed_line ParsedLine) UserApps {
	var ua UserApps

	ua.Lat = &parsed_line.lat
	ua.Lon = &parsed_line.lon
	ua.Apps = parsed_line.apps
	return ua
}

func dot_rename(file_path string) {
	head, fn := filepath.Split(file_path)
	err := os.Rename(file_path, filepath.Join(head, "."+fn))
	if err != nil {
		log.Fatal(err)
	}
}

func parse_line(line string) ParsedLine {
	var parsed ParsedLine
	var err error

	words := strings.Fields(line)
	if len(words) != 5 {
		log.Fatal("wrong line structure")
	}
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
		app, err := strconv.ParseUint(s, 10, 32)
		if err != nil {
			log.Fatal(err)
		}
		parsed.apps = append(parsed.apps, uint32(app))
	}
	return parsed
}

func upload_message(data_chan chan ParsedLine, done_chan chan bool,
	mc_connections map[string]*memcache.Client) {

	for {
		parsed_line, ok := <-data_chan
		if !ok {
			fmt.Println("data chan closed")
			done_chan <- true
			return
		}

		mc := mc_connections[parsed_line.dev_type]
		key := fmt.Sprintf("%s:%s", parsed_line.dev_type, parsed_line.dev_id)
		proto_msg := make_protobuf_struct(parsed_line)
		proto_msg_serialized, err := proto.Marshal(&proto_msg)
		if err != nil {
			log.Fatal(err)
		}

		n_try := 0
		for {
			err = mc.Set(&memcache.Item{Key: key, Value: proto_msg_serialized})
			if err == nil {
				break
			} else if n_try > 5 {
				log.Fatal(err)
			}
			n_try++
		}
	}
}

func process_one_file(file_path string, mc_connections map[string]*memcache.Client) {
	done_chan := make(chan bool)
	data_chan := make(chan ParsedLine)

	f, err := os.Open(file_path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}
	defer gr.Close()

	for i := 0; i < UPLOAD_WORKERS; i++ {
		go upload_message(data_chan, done_chan, mc_connections)
	}

	scanner := bufio.NewScanner(gr)
	for scanner.Scan() {
		line := scanner.Text()
		data_chan <- parse_line(line)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	close(data_chan)
	for i := 0; i < UPLOAD_WORKERS; i++ {
		<-done_chan
	}

	dot_rename(file_path)
}

func main() {
	mc_connections := map[string]*memcache.Client{
		"idfa": memcache.New("127.0.0.1:33013"),
		"gaid": memcache.New("127.0.0.1:33014"),
		"adid": memcache.New("127.0.0.1:33015"),
		"dvid": memcache.New("127.0.0.1:33016"),
	}

	files, err := filepath.Glob(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	for _, file_path := range files {
		process_one_file(file_path, mc_connections)
	}
}
