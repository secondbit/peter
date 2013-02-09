package peter

import (
	"secondbit.org/wendy"
	"sync"
)

type subscriptionMap struct {
	*sync.RWMutex
	items map[Topic][]wendy.NodeID
}

func newSubscriptionMap() *subscriptionMap {
	return &subscriptionMap{
		new(sync.RWMutex),
		map[Topic][]wendy.NodeID{},
	}
}

func (s *subscriptionMap) insert(t Topic, id wendy.NodeID) bool {
	s.Lock()
	defer s.Unlock()
	if ids, set := s.items[t]; set {
		for _, i := range ids {
			if id.Equals(i) {
				return false
			}
		}
		s.items[t] = append(s.items[t], id)
	} else {
		s.items[t] = []wendy.NodeID{id}
	}
	return true
}

func (s *subscriptionMap) remove(t Topic, id wendy.NodeID) (removed, empty bool) {
	s.Lock()
	defer s.Unlock()
	return s.unsafeRemove(t, id)
}

func (s *subscriptionMap) removeSubscriber(id wendy.NodeID) {
	s.Lock()
	defer s.Unlock()
	for topic, _ := range s.items {
		s.unsafeRemove(topic, id)
	}
}

func (s *subscriptionMap) unsafeRemove(t Topic, id wendy.NodeID) (removed, empty bool) {
	if ids, set := s.items[t]; set {
		for pos, i := range ids {
			if id.Equals(i) {
				empty := false
				if len(s.items[t]) == 1 {
					s.items[t] = []wendy.NodeID{}
					empty = true
				} else if pos == 0 {
					s.items[t] = s.items[t][1:]
				} else if pos+1 == len(s.items[t]) {
					s.items[t] = s.items[t][:pos]
				} else {
					s.items[t] = append(s.items[t][:pos], s.items[t][pos+1:]...)
				}
				return true, empty
			}
		}
	}
	return false, false
}

func (s *subscriptionMap) list(t Topic) []wendy.NodeID {
	s.RLock()
	defer s.RUnlock()
	return s.items[t]
}
