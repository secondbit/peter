package peter

import (
	"secondbit.org/wendy"
	"sync"
)

type parentMap struct {
	*sync.RWMutex
	items map[wendy.NodeID][]Topic
}

func newParentMap() *parentMap {
	return &parentMap{
		new(sync.RWMutex),
		map[wendy.NodeID][]Topic{},
	}
}

func (p *parentMap) insert(id wendy.NodeID, t Topic) bool {
	p.Lock()
	defer p.Unlock()
	if topics, set := p.items[id]; set {
		for _, topic := range topics {
			if topic == t {
				return false
			}
		}
		p.items[id] = append(p.items[id], t)
	} else {
		p.items[id] = []Topic{t}
	}
	return true
}

func (p *parentMap) remove(id wendy.NodeID, t Topic) {
	p.Lock()
	defer p.Unlock()
	if topics, set := p.items[id]; set {
		for pos, topic := range topics {
			if topic == t {
				if len(p.items[id]) == 1 {
					p.items[id] = []Topic{}
				} else if pos == 0 {
					p.items[id] = p.items[id][1:]
				} else if pos+1 == len(p.items[id]) {
					p.items[id] = p.items[id][:pos]
				} else {
					p.items[id] = append(p.items[id][:pos], p.items[id][pos+1:]...)
				}
				return
			}
		}
	}
	return
}

func (p *parentMap) list(id wendy.NodeID) []Topic {
	p.RLock()
	defer p.RUnlock()
	return p.items[id]
}
