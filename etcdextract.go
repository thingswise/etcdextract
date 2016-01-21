package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/coreos/etcd/client"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("etcdextract")

// Example format string. Everything except the message has a custom color
// which is dependent on the log level. Many fields have a custom output
// formatting too, eg. the time returns the hour down to the milli second.
var format = logging.MustStringFormatter(
	"%{color}%{time:15:04:05.000} %{shortfunc} ▶ %{level:.4s} %{id:03x}%{color:reset} %{message}",
)

var (
	debug        = flag.Bool("v", false, "verbose output")
	etcdEndpoint = flag.String("e", "http://127.0.0.1:2379", "etcd endpoint")
)

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s DOC_ROOTS INTERVAL URL\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  DOC_ROOTS - comma-separated list of etcd roots to extract\n")
	fmt.Fprintf(os.Stderr, "  INTERVAL  - interval in seconds to perform the extraction\n")
	fmt.Fprintf(os.Stderr, "  URL       - URL to post the JSON data to\n")
	flag.PrintDefaults()
}

func main() {

	sc := make(chan os.Signal, 1)
	signal.Notify(sc)
	go func() {
		s := <-sc
		ssig := s.(syscall.Signal)
		log.Error("Signal received: %s", ssig.String())
		os.Exit(128 + int(ssig))
	}()

	flag.Usage = Usage
	flag.Parse()

	if flag.NArg() != 3 {
		Usage()
		os.Exit(2)
	} else {
		docRoots := flag.Arg(0)
		intervalStr := flag.Arg(1)
		url := flag.Arg(2)

		roots := strings.Split(docRoots, ",")
		interval, err := strconv.Atoi(intervalStr)
		if err != nil {
			log.Fatal("Invalid interval value: %s", intervalStr)
		}

		cfg := client.Config{
			Endpoints: []string{*etcdEndpoint},
			Transport: client.DefaultTransport,
			// set timeout per request to fail fast when the target endpoint is unavailable
			HeaderTimeoutPerRequest: 5 * time.Second,
		}
		c, err := client.New(cfg)
		if err != nil {
			log.Fatal(err)
		}
		kapi := client.NewKeysAPI(c)

		for {
			run(kapi, roots, url)
			time.Sleep(time.Duration(interval) * time.Second)
		}

	}
}

type Request struct {
	Timestamp int64       `json:"timestamp"`
	Data      interface{} `json:"data"`
}

func run(kapi client.KeysAPI, roots []string, url string) {
	doc := make(map[string]interface{})
	for _, root := range roots {
		ctx, _ := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
		resp, err := kapi.Get(ctx, root, &client.GetOptions{Recursive: true})
		if err != nil {
			log.Error("Cannot get root: %s. Error: %s", root, err.Error())
			continue
		}
		if resp.Node != nil {
			merge(&doc, resp.Node)
		}
	}

	if url == "stdout://" {
		buf, err := json.MarshalIndent(&Request{
			Timestamp: time.Now().Unix(),
			Data:      doc,
		}, "", "  ")
		if err != nil {
			panic(err)
		}

		fmt.Printf("%s\n", string(buf))
	} else {
		buf, err := json.Marshal(&Request{
			Timestamp: time.Now().Unix(),
			Data:      doc,
		})
		if err != nil {
			panic(err)
		}

		httpResp, err := http.Post(url, "application/json", bytes.NewReader(buf))
		if err != nil {
			log.Error("Cannot send HTTP request: %s", err.Error())
		}

		if httpResp.StatusCode != 200 {
			log.Error("Error received from the HTTP endpoint: %s", httpResp.Status)
			defer httpResp.Body.Close()
			errBuf, err := ioutil.ReadAll(httpResp.Body)
			if err != nil {
				log.Error("Error response: %s", string(errBuf))
			}
			return
		}
	}
}

func merge(doc *map[string]interface{}, node *client.Node) {
	key := node.Key
	segments := strings.Split(key, "/")

	if len(segments) > 0 {
		if segments[0] == "" {
			var d *map[string]interface{} = doc
			var prev string = ""
			for _, s := range segments[1:] {
				if s == "" {
					break
				}

				if prev != "" {
					v, ok := (*d)[prev]
					if !ok {
						n := make(map[string]interface{})
						(*d)[prev] = n
						d = &n
					} else {
						p, ok := v.(map[string]interface{})
						if ok {
							d = &p
						} else {
							n := make(map[string]interface{})
							(*d)[prev] = n
							d = &n
						}
					}
				}
				prev = s
			}

			if node.Dir {
				for _, n := range node.Nodes {
					merge(doc, n)
				}
			} else {
				if prev != "" {
					(*d)[prev] = node.Value
				}
			}
		}
	}
}
