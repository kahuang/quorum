package quorum

type PeerFinder interface {
	Peers() []string
}

type DefaultPeerFinder struct {
	InitialPeerSet []string
}

func (d *DefaultPeerFinder) Peers() []string {
	return d.InitialPeerSet
}

func NewDefaultPeerFinder(initialPeerSet []string) *DefaultPeerFinder {
	return &DefaultPeerFinder{InitialPeerSet: initialPeerSet}
}
