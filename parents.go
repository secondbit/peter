package peter

import (
	"secondbit.org/wendy"
	"sort"
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

func (p *parentMap) topicsByID(id wendy.NodeID) []Topic {
	p.RLock()
	defer p.RUnlock()
	return p.items[id]
}

func (p *parentMap) topics() []Topic {
	p.RLock()
	defer p.RUnlock()
	topics := []Topic{}
	for _, t := range p.items {
		topics = append(topics, t...)
	}
	tmpTopics := topicSlice(topics)
	sort.Sort(tmpTopics)
	topics = []Topic(tmpTopics)
	result := []Topic{}
	for i, topic := range topics {
		if i > 0 && topic == topics[i-1] {
			continue
		}
		result = append(result, topic)
	}
	return result
}

func (p *parentMap) ids() []wendy.NodeID {
	p.RLock()
	defer p.RUnlock()
	keys := []wendy.NodeID{}
	for key, _ := range p.items {
		keys = append(keys, key)
	}
	return keys
}

func (p *parentMap) export() map[wendy.NodeID][]Topic {
	output := map[wendy.NodeID][]Topic{}
	p.RLock()
	defer p.RUnlock()
	for id, topics := range p.items {
		output[id] = make([]Topic, len(output[id]), (cap(output[id])+1)*2)
		copy(output[id], topics)
	}
	return output
}
