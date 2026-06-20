package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

type HTTPAPI struct {
	node   *HermesNode
	server *http.Server
}

func StartHTTPAPI(node *HermesNode, addr string) (*HTTPAPI, error) {
	api := &HTTPAPI{node: node}
	mux := http.NewServeMux()
	mux.HandleFunc("/put", api.handlePut)
	mux.HandleFunc("/get", api.handleGet)
	mux.HandleFunc("/delete", api.handleDelete)
	mux.HandleFunc("/scan", api.handleScan)
	mux.HandleFunc("/cluster/status", api.handleClusterStatus)
	mux.HandleFunc("/healthz", api.handleHealth)

	api.server = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	go func() {
		if err := api.server.Serve(listener); err != nil && err != http.ErrServerClosed {
			node.logger.Error("HTTP API error", err)
		}
	}()

	node.logger.Info("HTTP API started", map[string]interface{}{"addr": listener.Addr().String()})
	return api, nil
}

func (api *HTTPAPI) Stop(ctx context.Context) error {
	return api.server.Shutdown(ctx)
}

func (api *HTTPAPI) handlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Key == "" {
		writeError(w, http.StatusBadRequest, "key is required")
		return
	}
	if err := api.node.Put(r.Context(), req.Key, []byte(req.Value)); err != nil {
		writeError(w, statusForErr(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "OK"})
}

func (api *HTTPAPI) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "key is required")
		return
	}
	value, found, err := api.node.Get(r.Context(), key)
	if err != nil {
		writeError(w, statusForErr(err), err.Error())
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "key not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"key":   key,
		"value": string(value),
	})
}

func (api *HTTPAPI) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	key := r.URL.Query().Get("key")
	if key == "" {
		var req struct {
			Key string `json:"key"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		key = req.Key
	}
	if key == "" {
		writeError(w, http.StatusBadRequest, "key is required")
		return
	}
	if err := api.node.Delete(r.Context(), key); err != nil {
		writeError(w, statusForErr(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "OK"})
}

func (api *HTTPAPI) handleScan(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")
	if prefix != "" {
		start = prefix
		end = prefixEnd(prefix)
	}
	entries, err := api.node.Scan(r.Context(), start, end)
	if err != nil {
		writeError(w, statusForErr(err), err.Error())
		return
	}
	items := make([]map[string]string, 0, len(entries))
	for _, entry := range entries {
		items = append(items, map[string]string{
			"key":   entry.Key,
			"value": string(entry.Value),
		})
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"items": items})
}

func (api *HTTPAPI) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	status := api.node.Status()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"node_id":       status.NodeID,
		"leader":        status.LeaderID,
		"is_leader":     status.IsLeader,
		"nodes_alive":   status.ClusterSize,
		"nodes_total":   status.ClusterSize,
		"shards":        status.Shards,
		"term":          status.Term,
		"commit_index":  status.CommitIndex,
		"applied_index": status.AppliedIndex,
		"healthy":       status.Healthy,
	})
}

func (api *HTTPAPI) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "OK"})
}

func writeJSON(w http.ResponseWriter, code int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, code int, message string) {
	writeJSON(w, code, map[string]string{"error": message})
}

func statusForErr(err error) int {
	if err == nil {
		return http.StatusOK
	}
	if strings.HasPrefix(err.Error(), "not leader") {
		return http.StatusServiceUnavailable
	}
	return http.StatusInternalServerError
}

func prefixEnd(prefix string) string {
	if prefix == "" {
		return ""
	}
	bytes := []byte(prefix)
	for i := len(bytes) - 1; i >= 0; i-- {
		if bytes[i] < 0xff {
			bytes[i]++
			return string(bytes[:i+1])
		}
	}
	return fmt.Sprintf("%s\x00", prefix)
}
