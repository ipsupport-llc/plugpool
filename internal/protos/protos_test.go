package protos

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestNodeGroupRoundTrip(t *testing.T) {
	in := &NodeGroup{Id: "pool", MinSize: 0, MaxSize: 2}
	b, err := proto.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	out := &NodeGroup{}
	if err := proto.Unmarshal(b, out); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(in, out) {
		t.Fatalf("round trip: got %v, want %v", out, in)
	}
}
