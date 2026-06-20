package membership

import (
	"context"
	"sync"
	"time"

	pb "github.com/aadityabinodyadav/hermes/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GrpcSWIMTransport struct {
	nodeID    string
	recvCh    chan SWIMMessage
	resolver  func(string) string
	
	mu    sync.RWMutex
	conns map[string]pb.MembershipServiceClient
}

func NewGrpcSWIMTransport(nodeID string, resolver func(string) string) *GrpcSWIMTransport {
	return &GrpcSWIMTransport{
		nodeID:   nodeID,
		recvCh:   make(chan SWIMMessage, 1024),
		resolver: resolver,
		conns:    make(map[string]pb.MembershipServiceClient),
	}
}

func (t *GrpcSWIMTransport) getClient(target string) (pb.MembershipServiceClient, error) {
	addr := target
	if t.resolver != nil {
		if resolved := t.resolver(target); resolved != "" {
			addr = resolved
		}
	}

	t.mu.RLock()
	client, exists := t.conns[addr]
	t.mu.RUnlock()

	if exists {
		return client, nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	
	// Double check
	if client, exists := t.conns[addr]; exists {
		return client, nil
	}

	// Dial
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	client = pb.NewMembershipServiceClient(conn)
	t.conns[addr] = client
	return client, nil
}

func (t *GrpcSWIMTransport) Send(ctx context.Context, target string, msg SWIMMessage) error {
	client, err := t.getClient(target)
	if err != nil {
		return err
	}

	updates := EncodeUpdates(msg.Updates)

	switch msg.Type {
	case MsgPing:
		req := &pb.PingRequest{
			SenderId:       t.nodeID,
			SequenceNumber: msg.SeqNum,
			GossipUpdates:  updates,
		}
		
		// Synchronous RPC call
		resp, err := client.Ping(ctx, req)
		if err != nil {
			return err
		}

		// Inject ACK back to local SWIM
		t.recvCh <- SWIMMessage{
			Type:    MsgPingAck,
			From:    resp.SenderId,
			To:      t.nodeID,
			SeqNum:  resp.SequenceNumber,
			Updates: DecodeUpdates(resp.GossipUpdates),
		}

	case MsgPingReq:
		req := &pb.PingReqRequest{
			SenderId:       t.nodeID,
			TargetId:       msg.Target,
			SequenceNumber: msg.SeqNum,
		}

		resp, err := client.PingReq(ctx, req)
		if err != nil {
			return err
		}

		t.recvCh <- SWIMMessage{
			Type:    MsgPingReqAck,
			From:    target, // We got the ack from the helper
			To:      t.nodeID,
			Target:  msg.Target,
			SeqNum:  resp.SequenceNumber,
			Updates: DecodeUpdates(resp.GossipUpdates),
		}
	}
	return nil
}

func (t *GrpcSWIMTransport) Recv() <-chan SWIMMessage {
	return t.recvCh
}

// InjectIncoming allows the gRPC handlers to pass incoming messages to the local SWIM protocol
func (t *GrpcSWIMTransport) InjectIncoming(msg SWIMMessage) {
	select {
	case t.recvCh <- msg:
	default:
		// Drop if full
	}
}

func EncodeUpdates(updates []GossipUpdate) []*pb.Member {
	var pbUpdates []*pb.Member
	for _, u := range updates {
		state := pb.MemberState_ALIVE
		switch u.State {
		case StateSuspected:
			state = pb.MemberState_SUSPECTED
		case StateDead:
			state = pb.MemberState_DEAD
		case StateLeft:
			state = pb.MemberState_LEFT
		}
		
		pbUpdates = append(pbUpdates, &pb.Member{
			Info: &pb.NodeInfo{
				Id:      u.NodeID,
				Address: u.Address,
			},
			State:       state,
			Incarnation: u.Incarnation,
			LastSeen:    time.Now().UnixNano(),
		})
	}
	return pbUpdates
}

func DecodeUpdates(pbUpdates []*pb.Member) []GossipUpdate {
	var updates []GossipUpdate
	for _, u := range pbUpdates {
		if u.Info == nil {
			continue
		}
		
		state := StateAlive
		switch u.State {
		case pb.MemberState_SUSPECTED:
			state = StateSuspected
		case pb.MemberState_DEAD:
			state = StateDead
		case pb.MemberState_LEFT:
			state = StateLeft
		}
		
		updates = append(updates, GossipUpdate{
			NodeID:      u.Info.Id,
			Address:     u.Info.Address,
			State:       state,
			Incarnation: u.Incarnation,
		})
	}
	return updates
}
