package quorum

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

//TODO: pluggable store?
type fsmSnapshot struct {
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	return sink.Close()
}

func (f *fsmSnapshot) Release() {}

type Population struct {
	Memberlist int64
	Raft       int64
}

func (p *Population) Ratio() float64 {
	if p.Raft <= 0 {
		return float64(0)
	}
	return float64(p.Memberlist) / float64(p.Raft)
}

type PruneMap map[string]time.Time
type IdMap map[string]string

type QuorumNode struct {
	Config *Config

	Memberlist *memberlist.Memberlist
	Raft       *raft.Raft

	APIClient   *HTTPClient
	JoinRequest *JoinRequest

	Population *atomic.Value
	SplitBrain *int32
	pruneMap   *atomic.Value
	idMap      *atomic.Value

	metrics *Metrics
}

func (q *QuorumNode) Apply(l *raft.Log) interface{} {
	if q.Config.FSM != nil {
		return q.Config.FSM.Apply(l)
	}
	return true
}

func (q *QuorumNode) Snapshot() (raft.FSMSnapshot, error) {
	if q.Config.FSM != nil {
		return q.Config.FSM.Snapshot()
	}
	return &fsmSnapshot{}, nil
}

func (q *QuorumNode) Restore(rc io.ReadCloser) error {
	if q.Config.FSM != nil {
		return q.Config.FSM.Restore(rc)
	}
	return nil
}

func (q *QuorumNode) newRaft() error {
	raftConfig := raft.DefaultConfig()
	raftBind := q.Config.IP + ":" + q.Config.RaftPort
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	raftConfig.LocalID = raft.ServerID(hostname)
	addr, err := net.ResolveTCPAddr("tcp", raftBind)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(raftBind, addr, q.Config.RaftPoolSize, q.Config.RaftTimeout, os.Stderr)
	if err != nil {
		return err
	}
	snapshots, err := raft.NewFileSnapshotStore(q.Config.RaftDir, q.Config.RaftRetain, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(q.Config.RaftDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}
	ra, err := raft.NewRaft(raftConfig, q, logStore, logStore, snapshots, transport)
	if err != nil {
		return err
	}
	q.Raft = ra

	q.JoinRequest = &JoinRequest{ServerID: hostname, ServerAddress: addr.String()}

	if len(q.Config.PeerFinder.Peers()) <= 0 {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raftConfig.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		ra.BootstrapCluster(configuration)
	} else {
		ctx := context.Background()
		if err := q.tryJoinCluster(ctx, q.Config.PeerFinder.Peers()); err != nil {
			return err
		}
	}

	return nil
}

func (q *QuorumNode) tryJoinCluster(ctx context.Context, peerSet []string) error {
	var resp *JoinResponse
	// TODO: Revisit this logic of finding leader + joining? make more robust? this should be fine for now
	for _, peer := range peerSet {
		//TODO separate lists for memberlist/ raft with specified ports
		resp, err := q.tryJoinPeer(ctx, peer+":"+q.Config.Port)
		if err == nil {
			if resp.OK {
				break
			} else {
				for i := 0; i <= 3; i++ {
					if resp.Leader != "" {
						resp, err = q.tryJoinPeer(ctx, resp.Leader)
						if err != nil {
							break
						}
						if resp.OK {
							break
						}
					} else {
						break
					}
				}
			}
		}
	}
	if resp != nil && !resp.OK {
		return fmt.Errorf("Error, was unable to join raft peer group")
	}
	return nil
}

func (q *QuorumNode) tryJoinPeer(ctx context.Context, peer string) (*JoinResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, q.Config.APIRequestTimeout)
	defer cancel()
	return q.APIClient.Join(ctx, peer, q.JoinRequest)
}

func (q *QuorumNode) tryLeaveCluster(ctx context.Context) (*LeaveResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, q.Config.APIRequestTimeout)
	defer cancel()
	leader, err := q.GetLeader()
	if err != nil {
		return nil, err
	}
	return q.APIClient.Leave(ctx, leader, &LeaveRequest{ServerID: q.JoinRequest.ServerID})
}

func (q *QuorumNode) newMemberlist() error {
	list, err := memberlist.Create(q.Config.MemberlistConfig)
	if err != nil {
		return err
	}
	_, err = list.Join(q.Config.PeerFinder.Peers())
	q.Memberlist = list
	return nil
}

func (q *QuorumNode) Join(serverId string, serverAddress string) error {
	future := q.Raft.AddVoter(raft.ServerID(serverId), raft.ServerAddress(serverAddress), 0, 0)
	err := future.Error()
	return err
}

func (q *QuorumNode) Remove(serverId string) error {
	future := q.Raft.RemoveServer(raft.ServerID(serverId), 0, 0)
	err := future.Error()
	return err
}

func (q *QuorumNode) GetLeader() (string, error) {
	leader := string(q.Raft.Leader())
	if leader == "" {
		return "", fmt.Errorf("Unknown leader at this time")
	}
	// TODO: generic out the port info?
	return ipFromAddress(leader) + ":" + q.Config.Port, nil
}

func (q *QuorumNode) GetPopulation() Population {
	return q.Population.Load().(Population)
}

func (q *QuorumNode) IsSplitBrain() bool {
	return atomic.LoadInt32(q.SplitBrain) == 1
}

func (q *QuorumNode) enterSplitBrain() {
	atomic.StoreInt32(q.SplitBrain, 1)
	q.metrics.SplitBrain(1)
}

func (q *QuorumNode) exitSplitBrain() {
	atomic.StoreInt32(q.SplitBrain, 0)
	q.metrics.SplitBrain(0)
}

func (q *QuorumNode) getPruneMap() PruneMap {
	return q.pruneMap.Load().(PruneMap)
}

func (q *QuorumNode) getIdMap() IdMap {
	return q.idMap.Load().(IdMap)
}

func ipFromAddress(address string) string {
	return strings.Split(address, ":")[0]
}

// This function keeps track of two things:
//     1. How many members are considered alive in Memberlist vs Raft
//        which is stored in Population. The ratio can be used to determine
//        if the quorum cluster is split brained or not.
//     2. For every member in out raftConfig, how many "ticks" they
//        have been dead/missing for. This is used internally to remove
//        nodes from the cluster that have been dead/missing for too long.
func (q *QuorumNode) quorumStateLoop(ctx context.Context) {
	for true {
		ticker := time.NewTicker(q.Config.CheckStatePeriod)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			raftConfigFuture := q.Raft.GetConfiguration()
			if err := raftConfigFuture.Error(); err != nil {
				log.Printf("Unable to get raft config: %v", err)
				break
			}
			raftConfig := raftConfigFuture.Configuration()
			memberlistMembers := q.Memberlist.Members()

			membersMap := make(map[string]bool)
			for _, member := range memberlistMembers {
				membersMap[member.Addr.String()] = true
			}

			pruneMap := q.getPruneMap()
			newMap := make(PruneMap)
			idMap := make(IdMap)

			for _, server := range raftConfig.Servers {
				ip := ipFromAddress(string(server.Address))
				idMap[ip] = string(server.ID)
				if val, ok := pruneMap[ip]; ok {
					newMap[ip] = val
				} else {
					newMap[ip] = time.Now()
				}
				if _, ok := membersMap[ip]; ok {
					delete(newMap, ip)
				}
			}
			q.pruneMap.Store(newMap)
			q.idMap.Store(idMap)

			var numRaftServers int64
			// If we've been removed from the raft configuration, then we are
			// not part of the consensus group and there are 0 raft servers
			if _, ok := idMap[ipFromAddress(q.JoinRequest.ServerAddress)]; !ok {
				numRaftServers = int64(0)
			} else {
				numRaftServers = int64(len(raftConfig.Servers))
			}
			newPopulation := Population{
				Raft:       numRaftServers,
				Memberlist: int64(len(memberlistMembers)),
			}
			q.Population.Store(newPopulation)
			q.metrics.RaftMembers(newPopulation.Raft)
			q.metrics.MemberlistMembers(newPopulation.Memberlist)
			if newPopulation.Ratio() <= q.Config.QuorumRatio {
				q.enterSplitBrain()
			} else {
				q.exitSplitBrain()
			}
		}
	}
}

func (q *QuorumNode) isLeader() bool {
	return q.Raft.State() == raft.Leader
}

func (q *QuorumNode) leaderLoop(ctx context.Context) {
	for true {
		ticker := time.NewTicker(q.Config.LeaderLoopPeriod)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !q.isLeader() {
				continue
			}
			pruneMap := q.getPruneMap()
			idMap := q.getIdMap()
			for ipaddr, deadSince := range pruneMap {
				if time.Now().Sub(deadSince) >= q.Config.PruneDuration {
					if serverId, ok := idMap[ipaddr]; ok {
						log.Printf("Removing server {id: %s, addr: %s} from raft configuration due to being dead for more than %s", ipaddr, serverId, q.Config.PruneDuration.String())
						q.Remove(serverId)
					}
				}
			}
		}
	}
}

func (q *QuorumNode) rejoinLoop(ctx context.Context) {
	for true {
		ticker := time.NewTicker(time.Second * 5)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			idMap := q.getIdMap()
			if _, ok := idMap[ipFromAddress(q.JoinRequest.ServerAddress)]; !ok {
				peers := q.Config.PeerFinder.Peers()
				log.Printf("We got removed from the cluster - trying to rejoin peers: %v", peers)
				if err := q.tryJoinCluster(ctx, peers); err != nil {
					log.Printf("Failed to rejoin cluster: %v", err)
				}
			}
			if q.IsSplitBrain() {
				peers := q.Config.PeerFinder.Peers()
				q.Memberlist.Join(peers)
			}
		}
	}
}

func (q *QuorumNode) Shutdown(ctx context.Context) error {
	var successfulLeave bool
	for i := 0; i < 3; i++ {
		resp, err := q.tryLeaveCluster(ctx)
		if err != nil {
			return err
		}
		if resp.OK {
			successfulLeave = true
			break
		}
	}

	if successfulLeave {
		for i := 0; i < 3; i++ {
			select {
			case <-ctx.Done():
				log.Printf("Context is done, did not confirm if configuration was replicated")
				goto SHUTDOWN
			case <-time.NewTicker(time.Millisecond * 100).C:
				configFuture := q.Raft.GetConfiguration()
				if err := configFuture.Error(); err != nil {
					log.Printf("Could not get configuration: %v", err)
					break
				}
				config := configFuture.Configuration()
				if len(config.Servers) == 0 {
					log.Printf("Exited cluster gracefully, proceeding with shutdown")
					goto SHUTDOWN
				}
			}
		}
		log.Printf("Did not exit cluster gracefully, proceeding with shutdown")
	} else {
		log.Printf("Did not exit cluster gracefully, proceeding with shutdown")
	}

SHUTDOWN:
	q.Raft.Shutdown()
	q.Memberlist.Leave(time.Second)
	q.Memberlist.Shutdown()
	return nil
}

func NewQuorumNode(config *Config) (*QuorumNode, error) {
	var population atomic.Value
	var pruneMap atomic.Value
	var idMap atomic.Value
	var splitBrain int32
	metrics, err := NewMetrics()
	if err != nil {
		return nil, err
	}
	quorum := &QuorumNode{
		Config:     config,
		APIClient:  NewHTTPClient(),
		Population: &population,
		pruneMap:   &pruneMap,
		idMap:      &idMap,
		SplitBrain: &splitBrain,
		metrics:    metrics,
	}
	quorum.Population.Store(Population{Memberlist: int64(1), Raft: int64(1)})
	quorum.pruneMap.Store(make(PruneMap))
	quorum.idMap.Store(make(IdMap))
	if err := quorum.newRaft(); err != nil {
		return nil, err
	}
	if err := quorum.newMemberlist(); err != nil {
		return nil, err
	}
	ctx := context.Background()
	go quorum.quorumStateLoop(ctx)
	go quorum.leaderLoop(ctx)
	go quorum.rejoinLoop(ctx)
	return quorum, nil
}
