package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

var serverAddr = "http://localhost:7000"

func main() {
	if addr := os.Getenv("HERMES_HTTP_ADDR"); addr != "" {
		if !strings.HasPrefix(addr, "http://") {
			addr = "http://" + addr
		}
		serverAddr = addr
	}

	if len(os.Args) < 2 {
		runREPL()
		return
	}

	err := runCommand(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runREPL() {
	fmt.Println("Hermes CLI REPL")
	fmt.Printf("Connecting to %s\n", serverAddr)
	fmt.Println("Commands: put <key> <val>, get <key>, delete <key>, scan <prefix>, cluster status, exit")
	
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("hermes> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if line == "exit" || line == "quit" {
			break
		}
		
		args := strings.Fields(line)
		err := runCommand(args)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		}
	}
}

func runCommand(args []string) error {
	cmd := args[0]
	switch cmd {
	case "put":
		if len(args) < 3 {
			return fmt.Errorf("usage: put <key> <value>")
		}
		return doPut(args[1], args[2])
	case "get":
		if len(args) < 2 {
			return fmt.Errorf("usage: get <key>")
		}
		return doGet(args[1])
	case "delete":
		if len(args) < 2 {
			return fmt.Errorf("usage: delete <key>")
		}
		return doDelete(args[1])
	case "scan":
		prefix := ""
		if len(args) >= 3 && args[1] == "--prefix" {
			prefix = args[2]
		} else if len(args) >= 2 {
			prefix = args[1] // assume just scan <prefix> as in REPL
		}
		return doScan(prefix)
	case "cluster":
		if len(args) >= 2 && args[1] == "status" {
			return doClusterStatus()
		}
		return fmt.Errorf("unknown cluster command. Use 'cluster status'")
	default:
		return fmt.Errorf("unknown command: %s", cmd)
	}
}

func doPut(key, value string) error {
	reqBody, _ := json.Marshal(map[string]string{
		"key":   key,
		"value": value,
	})
	resp, err := http.Post(serverAddr+"/put", "application/json", bytes.NewReader(reqBody))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned %s: %s", resp.Status, readBody(resp.Body))
	}
	fmt.Println("OK")
	return nil
}

func doGet(key string) error {
	resp, err := http.Get(serverAddr + "/get?key=" + key)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned %s: %s", resp.Status, readBody(resp.Body))
	}

	var res map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return err
	}
	fmt.Println(res["value"])
	return nil
}

func doDelete(key string) error {
	req, err := http.NewRequest(http.MethodDelete, serverAddr+"/delete?key="+key, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned %s: %s", resp.Status, readBody(resp.Body))
	}
	fmt.Println("OK")
	return nil
}

func doScan(prefix string) error {
	url := serverAddr + "/scan"
	if prefix != "" {
		url += "?prefix=" + prefix
	}
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned %s: %s", resp.Status, readBody(resp.Body))
	}

	var res struct {
		Items []map[string]string `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return err
	}

	for _, item := range res.Items {
		fmt.Printf("%-11s = %s\n", item["key"], item["value"])
	}
	return nil
}

func doClusterStatus() error {
	resp, err := http.Get(serverAddr + "/cluster/status")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned %s: %s", resp.Status, readBody(resp.Body))
	}

	var status map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return err
	}
	
	nodeID := status["node_id"]
	leader := status["leader"]
	if status["is_leader"] == true {
		leader = fmt.Sprintf("%v (you)", nodeID)
	}

	fmt.Printf("Leader: %v\n", leader)
	fmt.Printf("Nodes:  %v/%v alive\n", status["nodes_alive"], status["nodes_total"])
	fmt.Printf("Shards: %v\n", status["shards"])
	return nil
}

func readBody(r io.Reader) string {
	b, _ := io.ReadAll(r)
	var res map[string]string
	if err := json.Unmarshal(b, &res); err == nil && res["error"] != "" {
		return res["error"]
	}
	return strings.TrimSpace(string(b))
}
