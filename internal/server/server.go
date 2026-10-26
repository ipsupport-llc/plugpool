// Package server serves cluster-autoscaler's externalgrpc CloudProvider
// service over plugpool's node groups.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/ipsupport-llc/plugpool/internal/pool"
	"github.com/ipsupport-llc/plugpool/internal/protos"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// Server implements protos.CloudProviderServer. Pricing and per-group
// autoscaling options are left Unimplemented, which cluster-autoscaler
// reads as "use the defaults".
type Server struct {
	protos.UnimplementedCloudProviderServer
	pools map[string]*pool.Pool
}

// New serves the given pools, keyed by node group ID.
func New(pools map[string]*pool.Pool) *Server { return &Server{pools: pools} }

func (s *Server) pool(id string) (*pool.Pool, error) {
	p, ok := s.pools[id]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "node group %q not found", id)
	}
	return p, nil
}

func nodeGroup(p *pool.Pool) *protos.NodeGroup {
	g := p.Group()
	return &protos.NodeGroup{Id: g.ID, MinSize: int32(g.MinSize), MaxSize: int32(g.MaxSize)}
}

// NodeGroups lists every configured group.
func (s *Server) NodeGroups(context.Context, *protos.NodeGroupsRequest) (*protos.NodeGroupsResponse, error) {
	r := &protos.NodeGroupsResponse{}
	for _, p := range s.pools {
		r.NodeGroups = append(r.NodeGroups, nodeGroup(p))
	}
	return r, nil
}

// NodeGroupForNode returns the node's group, or a group with an empty id
// for nodes plugpool does not manage.
func (s *Server) NodeGroupForNode(_ context.Context, req *protos.NodeGroupForNodeRequest) (*protos.NodeGroupForNodeResponse, error) {
	n := req.GetNode()
	for _, p := range s.pools {
		if _, ok := p.NodeFor(n.GetProviderID(), n.GetName()); ok {
			return &protos.NodeGroupForNodeResponse{NodeGroup: nodeGroup(p)}, nil
		}
	}
	return &protos.NodeGroupForNodeResponse{NodeGroup: &protos.NodeGroup{}}, nil
}

// GPULabel: no GPU label in v1.
func (s *Server) GPULabel(context.Context, *protos.GPULabelRequest) (*protos.GPULabelResponse, error) {
	return &protos.GPULabelResponse{}, nil
}

// GetAvailableGPUTypes: none in v1.
func (s *Server) GetAvailableGPUTypes(context.Context, *protos.GetAvailableGPUTypesRequest) (*protos.GetAvailableGPUTypesResponse, error) {
	return &protos.GetAvailableGPUTypesResponse{GpuTypes: map[string]*anypb.Any{}}, nil
}

// Cleanup has nothing to release; running scale-downs are stopped by main.
func (s *Server) Cleanup(context.Context, *protos.CleanupRequest) (*protos.CleanupResponse, error) {
	return &protos.CleanupResponse{}, nil
}

// Refresh re-reads every pool.
func (s *Server) Refresh(ctx context.Context, _ *protos.RefreshRequest) (*protos.RefreshResponse, error) {
	var errs []error
	for _, p := range s.pools {
		if err := p.Refresh(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &protos.RefreshResponse{}, nil
}

// NodeGroupTargetSize returns the group's Creating + Running nodes.
func (s *Server) NodeGroupTargetSize(_ context.Context, req *protos.NodeGroupTargetSizeRequest) (*protos.NodeGroupTargetSizeResponse, error) {
	p, err := s.pool(req.GetId())
	if err != nil {
		return nil, err
	}
	return &protos.NodeGroupTargetSizeResponse{TargetSize: int32(p.TargetSize())}, nil
}

// NodeGroupIncreaseSize powers on delta nodes.
func (s *Server) NodeGroupIncreaseSize(ctx context.Context, req *protos.NodeGroupIncreaseSizeRequest) (*protos.NodeGroupIncreaseSizeResponse, error) {
	p, err := s.pool(req.GetId())
	if err != nil {
		return nil, err
	}
	if err := p.IncreaseSize(ctx, int(req.GetDelta())); err != nil {
		slog.Warn("increase size failed", "group", req.GetId(), "delta", req.GetDelta(), "err", err)
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &protos.NodeGroupIncreaseSizeResponse{}, nil
}

// NodeGroupDeleteNodes starts the scale-down of the given nodes.
func (s *Server) NodeGroupDeleteNodes(ctx context.Context, req *protos.NodeGroupDeleteNodesRequest) (*protos.NodeGroupDeleteNodesResponse, error) {
	p, err := s.pool(req.GetId())
	if err != nil {
		return nil, err
	}
	var names []string
	for _, n := range req.GetNodes() {
		name, ok := p.NodeFor(n.GetProviderID(), n.GetName())
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "node %q (%s) is not in group %s", n.GetName(), n.GetProviderID(), req.GetId())
		}
		names = append(names, name)
	}
	if err := p.DeleteNodes(ctx, names); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &protos.NodeGroupDeleteNodesResponse{}, nil
}

// NodeGroupDecreaseTargetSize drops nodes that have not registered yet.
func (s *Server) NodeGroupDecreaseTargetSize(ctx context.Context, req *protos.NodeGroupDecreaseTargetSizeRequest) (*protos.NodeGroupDecreaseTargetSizeResponse, error) {
	p, err := s.pool(req.GetId())
	if err != nil {
		return nil, err
	}
	if err := p.DecreaseTargetSize(ctx, int(req.GetDelta())); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &protos.NodeGroupDecreaseTargetSizeResponse{}, nil
}

var instanceStates = map[pool.State]protos.InstanceStatus_InstanceState{
	pool.Creating: protos.InstanceStatus_instanceCreating,
	pool.Running:  protos.InstanceStatus_instanceRunning,
	pool.Deleting: protos.InstanceStatus_instanceDeleting,
}

// NodeGroupNodes lists the group's instances that are not off.
func (s *Server) NodeGroupNodes(_ context.Context, req *protos.NodeGroupNodesRequest) (*protos.NodeGroupNodesResponse, error) {
	p, err := s.pool(req.GetId())
	if err != nil {
		return nil, err
	}
	r := &protos.NodeGroupNodesResponse{}
	for _, i := range p.Instances() {
		r.Instances = append(r.Instances, &protos.Instance{Id: i.ID, Status: &protos.InstanceStatus{InstanceState: instanceStates[i.State]}})
	}
	return r, nil
}

// NodeGroupTemplateNodeInfo returns the group's template node.
func (s *Server) NodeGroupTemplateNodeInfo(_ context.Context, req *protos.NodeGroupTemplateNodeInfoRequest) (*protos.NodeGroupTemplateNodeInfoResponse, error) {
	p, err := s.pool(req.GetId())
	if err != nil {
		return nil, err
	}
	b, err := templateNode(p.Group()).Marshal()
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("marshal template node: %v", err))
	}
	// The DRA devices a new machine would publish (snapshots of the group's
	// machines). cluster-autoscaler 1.37+ uses them to scale up for pods with
	// ResourceClaims; older versions ignore the field.
	resp := &protos.NodeGroupTemplateNodeInfoResponse{NodeBytes: b}
	for _, s := range p.TemplateSlices() {
		sb, err := s.Marshal()
		if err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("marshal template resource slice: %v", err))
		}
		resp.ResourceSliceBytes = append(resp.ResourceSliceBytes, sb)
	}
	return resp, nil
}
