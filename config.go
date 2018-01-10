package quorum

import (
	"fmt"
	"net"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
)

type Config struct {
	// Address which other nodes can reach this one from
	IP string
	// Port to expose HTTP API on
	Port string

	// Config to pass into Memberlist
	MemberlistConfig *memberlist.Config

	// Directory to store persistent raft data
	RaftDir string
	// Port for the raft library to listen on
	RaftPort string
	// Pool size of the underlying TCP Transport layer
	RaftPoolSize int
	// Timeout for each request of the underlying TCP Transport layer
	RaftTimeout time.Duration
	// Number of snapshots to retain
	RaftRetain int
	// Config to pass into Raft
	RaftConfig *raft.Config

	// Interface that will return the set of peers to join. This is also
	// used when trying to rejoin the cluster when we have been removed
	// or we are in split brain.
	PeerFinder PeerFinder

	// If the ratio of memberlist members to raft members is less than or
	// equal to this ratio, we are in split-brain. Typical this is set to
	// 0.5 which allows a majority of nodes to keep operating.
	QuorumRatio float64

	// How often to check the state of the quorum cluster and detect
	// split-brain
	CheckStatePeriod time.Duration
	// How often the leader checks membership info and removes dead servers
	// from the raft configuration
	LeaderLoopPeriod time.Duration
	// Timeout for requests to the cluster using the HTTP API
	APIRequestTimeout time.Duration
	// How long after a node is dead in memberlist do we prune it from the
	// raft group
	PruneDuration time.Duration
}

func NewDefaultConfig(initialPeerSet []string) (*Config, error) {
	//TODO: generic this out, pass in option for interface to check
	// or pass in ip address
	myip := ""
	iface, err := net.InterfaceByName("eth0")
	if err != nil {
		return nil, err
	}
	addrs, err := iface.Addrs()
	if err != nil {
		return nil, err
	}
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				myip = ipnet.IP.String()
				break
			}
		}
	}

	if myip == "" {
		return nil, fmt.Errorf("Could not determine own ipaddress")
	}

	return &Config{
		IP:                myip,
		Port:              "8088",
		MemberlistConfig:  memberlist.DefaultWANConfig(),
		RaftDir:           "/tmp/",
		RaftPort:          "12000",
		RaftConfig:        raft.DefaultConfig(),
		PeerFinder:        NewDefaultPeerFinder(initialPeerSet),
		QuorumRatio:       float64(0.5),
		CheckStatePeriod:  time.Second,
		LeaderLoopPeriod:  time.Second * 5,
		APIRequestTimeout: time.Second * 5,
		RaftPoolSize:      3,
		RaftTimeout:       time.Second * 10,
		RaftRetain:        2,
		PruneDuration:     time.Second * 30,
	}, nil
}
