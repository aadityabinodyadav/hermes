// pkg/server/handlers.go
package server

// gRPC service handlers for HermesNode
// These implement the proto-generated server interfaces by embedding
// the Unimplemented* base types for forward compatibility, then
// delegating to the node's internal components.

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"

	pb "github.com/aadityabinodyadav/hermes/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ─────────────────────────────────────────────────────────────────────────────
// KV HANDLER
// ─────────────────────────────────────────────────────────────────────────────

// kvHandler implements pb.HermesKVServer
type kvHandler struct {
	pb.UnimplementedHermesKVServer
	node *HermesNode
}

func (h *kvHandler) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if req.Key == "" {
		return &pb.PutResponse{Error: pb.ErrorCode_INVALID_REQUEST, ErrorMsg: "key is required"}, nil
	}
	if err := h.node.Put(ctx, req.Key, req.Value); err != nil {
		return &pb.PutResponse{
			Error:      errorCode(err),
			ErrorMsg:   err.Error(),
			LeaderHint: h.node.Leader(),
		}, nil
	}
	return &pb.PutResponse{Error: pb.ErrorCode_OK}, nil
}

func (h *kvHandler) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if req.Key == "" {
		return &pb.GetResponse{Error: pb.ErrorCode_INVALID_REQUEST, ErrorMsg: "key is required"}, nil
	}
	value, found, err := h.node.Get(ctx, req.Key)
	if err != nil {
		return &pb.GetResponse{
			Error:      errorCode(err),
			ErrorMsg:   err.Error(),
			LeaderHint: h.node.Leader(),
		}, nil
	}
	if !found {
		return &pb.GetResponse{Error: pb.ErrorCode_KEY_NOT_FOUND, Found: false}, nil
	}
	return &pb.GetResponse{
		Error: pb.ErrorCode_OK,
		Value: value,
		Found: found,
	}, nil
}

func (h *kvHandler) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if req.Key == "" {
		return &pb.DeleteResponse{Error: pb.ErrorCode_INVALID_REQUEST, ErrorMsg: "key is required"}, nil
	}
	if err := h.node.Delete(ctx, req.Key); err != nil {
		return &pb.DeleteResponse{
			Error:      errorCode(err),
			ErrorMsg:   err.Error(),
			LeaderHint: h.node.Leader(),
		}, nil
	}
	return &pb.DeleteResponse{Error: pb.ErrorCode_OK}, nil
}

func (h *kvHandler) Scan(ctx context.Context, req *pb.ScanRequest) (*pb.ScanResponse, error) {
	entries, err := h.node.Scan(ctx, req.StartKey, req.EndKey)
	if err != nil {
		return &pb.ScanResponse{Error: errorCode(err), ErrorMsg: err.Error()}, nil
	}

	items := make([]*pb.KeyValue, 0, len(entries))
	for _, entry := range entries {
		items = append(items, &pb.KeyValue{
			Key:   entry.Key,
			Value: entry.Value,
		})
	}

	return &pb.ScanResponse{Error: pb.ErrorCode_OK, Items: items}, nil
}

// encodeKVCommand encodes a key-value command for Raft proposal
// Uses the same format as storage.encodeCommand
func encodeKVCommand(key string, value []byte, deleted bool) []byte {
	keyBytes := []byte(key)
	size := 1 + 4 + len(keyBytes) + 4 + len(value)
	buf := make([]byte, size)

	offset := 0
	if deleted {
		buf[0] = 1
	}
	offset++

	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(keyBytes)))
	offset += 4
	copy(buf[offset:], keyBytes)
	offset += len(keyBytes)

	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(value)))
	offset += 4
	copy(buf[offset:], value)

	return buf
}

func errorCode(err error) pb.ErrorCode {
	switch {
	case err == nil:
		return pb.ErrorCode_OK
	default:
		msg := err.Error()
		if msg == "rate limit exceeded" {
			return pb.ErrorCode_TIMEOUT
		}
		if strings.HasPrefix(msg, "not leader") {
			return pb.ErrorCode_NOT_LEADER
		}
		return pb.ErrorCode_STORAGE_ERROR
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// RAFT HANDLER
// ─────────────────────────────────────────────────────────────────────────────

// raftHandler implements pb.RaftServiceServer
type raftHandler struct {
	pb.UnimplementedRaftServiceServer
	node *HermesNode
}

func (h *raftHandler) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	fmt.Printf("[%s] received AppendEntries from %s\n", h.node.config.NodeID, req.LeaderId)
	return &pb.AppendEntriesResponse{
		Term:    req.Term,
		Success: true,
	}, nil
}

func (h *raftHandler) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	fmt.Printf("[%s] received RequestVote from %s\n", h.node.config.NodeID, req.CandidateId)
	return &pb.RequestVoteResponse{
		Term:        req.Term,
		VoteGranted: false,
	}, nil
}

func (h *raftHandler) TimeoutNow(ctx context.Context, req *pb.TimeoutNowRequest) (*pb.TimeoutNowResponse, error) {
	fmt.Printf("[%s] received TimeoutNow\n", h.node.config.NodeID)
	return &pb.TimeoutNowResponse{}, nil
}

func (h *raftHandler) InstallSnapshot(stream grpc.ClientStreamingServer[pb.InstallSnapshotRequest, pb.InstallSnapshotResponse]) error {
	fmt.Printf("[%s] received InstallSnapshot stream\n", h.node.config.NodeID)
	return status.Error(codes.Unimplemented, "InstallSnapshot not yet implemented")
}

// ─────────────────────────────────────────────────────────────────────────────
// MEMBERSHIP HANDLER
// ─────────────────────────────────────────────────────────────────────────────

// membershipHandler implements pb.MembershipServiceServer
type membershipHandler struct {
	pb.UnimplementedMembershipServiceServer
	node *HermesNode
}

func (h *membershipHandler) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{
		SenderId: h.node.config.NodeID,
	}, nil
}

func (h *membershipHandler) PingReq(ctx context.Context, req *pb.PingReqRequest) (*pb.PingReqResponse, error) {
	return &pb.PingReqResponse{
		Reached: true,
	}, nil
}

func (h *membershipHandler) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	fmt.Printf("[%s] received Join request\n", h.node.config.NodeID)
	return &pb.JoinResponse{
		Accepted: true,
	}, nil
}

func (h *membershipHandler) Leave(ctx context.Context, req *pb.LeaveRequest) (*pb.LeaveResponse, error) {
	fmt.Printf("[%s] received Leave from %s\n", h.node.config.NodeID, req.NodeId)
	return &pb.LeaveResponse{
		Success: true,
	}, nil
}

func (h *membershipHandler) GetClusterState(ctx context.Context, req *pb.ClusterStateRequest) (*pb.ClusterStateResponse, error) {
	alive := h.node.memberMgr.AliveMembers()
	members := make([]*pb.Member, 0, len(alive))
	for _, m := range alive {
		members = append(members, &pb.Member{
			Info: &pb.NodeInfo{
				Id:      m.NodeID,
				Address: m.Address,
			},
		})
	}
	return &pb.ClusterStateResponse{
		Members: members,
	}, nil
}
