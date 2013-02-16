package peter

import (
	"encoding/hex"
	"log"
	"os"
	"secondbit.org/wendy"
)

const (
	MSG_SUBSCRIBE   = byte(17)
	MSG_UNSUBSCRIBE = byte(18)
	MSG_EVENT       = byte(19)
)

const (
	LogLevelDebug = iota
	LogLevelWarn
	LogLevelError
)

type Topic string

type Peter struct {
	subscriptions *subscriptionMap
	parents       *parentMap
	cluster       *wendy.Cluster
	logLevel      int
	log           *log.Logger
}

// New creates a new instance of Peter, complete with a Node and the underlying Wendy Cluster, and registers itself to receive callbacks from the Cluster events.
func New(id wendy.NodeID, localIP, globalIP, region string, port int) *Peter {
	node := wendy.NewNode(id, localIP, globalIP, region, port)
	cluster := wendy.NewCluster(node, nil)
	peter := &Peter{
		subscriptions: newSubscriptionMap(),
		parents:       newParentMap(),
		cluster:       cluster,
		log:           log.New(os.Stdout, "peter("+id.String()+") ", log.LstdFlags),
		logLevel:      LogLevelWarn,
	}
	cluster.RegisterCallback(peter)
	return peter
}

// Listen starts the current Node listening for messages. A Node must then use the Join method to join a Cluster.
func (p *Peter) Listen() error {
	return p.cluster.Listen()
}

// Stop causes the current Node to exit the Cluster after it attempts to notify known Nodes of its departure, resulting in a graceful departure.
func (p *Peter) Stop() {
	p.cluster.Stop()
}

// Kill causes the current Node to immediately exit the Cluster, with no attempt at a graceful departure.
func (p *Peter) Kill() {
	p.cluster.Kill()
}

// Join includes the current Node in the Cluster that the Node specified by the provided IP and port is part of.
func (p *Peter) Join(ip string, port int) error {
	return p.cluster.Join(ip, port)
}

// Subscribe broadcasts the current Node's interest in a Topic, allowing it to receive future event notifications from that Topic.
func (p *Peter) Subscribe(t Topic) error {
	key, err := wendy.NodeIDFromBytes([]byte(string(t)))
	if err != nil {
		return err
	}
	msg := p.cluster.NewMessage(MSG_SUBSCRIBE, key, []byte{})
	err = p.cluster.Send(msg)
	return err
}

// Unsubscribe broadcasts that the current Node is no longer interested in a Topic, preventing it from receiving future event notifications for that Topic.
func (p *Peter) Unsubscribe(t Topic) error {
	key, err := wendy.NodeIDFromBytes([]byte(string(t)))
	if err != nil {
		return err
	}
	msg := p.cluster.NewMessage(MSG_UNSUBSCRIBE, key, []byte{})
	err = p.cluster.Send(msg)
	return err
}

// Broadcast sends an event notification for a Topic that will be sent to every Node subscribed to that Topic.
func (p *Peter) Broadcast(t Topic, body []byte) error {
	key, err := wendy.NodeIDFromBytes([]byte(string(t)))
	if err != nil {
		return err
	}
	msg := p.cluster.NewMessage(MSG_EVENT, key, body)
	err = p.cluster.Send(msg)
	return err
}

// OnError fulfills the wendy.Application interface. It will be called when the cluster encounters an error that it is unable to handle.
func (p *Peter) OnError(err error) {
}

// OnDeliver fulfills the wendy.Application interface. It will be called when a message is at the end of its routing path. Peter uses it to build the subscription tree.
func (p *Peter) OnDeliver(msg wendy.Message) {
	val := Topic(string(msg.Value))
	switch msg.Purpose {
	case MSG_SUBSCRIBE:
		p.subscriptions.insert(val, msg.Sender.ID)
		break
	case MSG_UNSUBSCRIBE:
		p.subscriptions.remove(val, msg.Sender.ID)
		break
	case MSG_EVENT:
		p.notifySubscribers(val)
		break
	}
}

// OnForward fulfills the wendy.Application interface. It will be called when a message is about to be passed on to the next Node in its routing path. Peter uses it to build the subscription tree, and may prematurely end the message's routing path.
func (p *Peter) OnForward(msg *wendy.Message, nextId wendy.NodeID) bool {
	val := Topic(string(msg.Value))
	switch msg.Purpose {
	case MSG_SUBSCRIBE:
		inserted := p.subscriptions.insert(val, msg.Sender.ID)
		if inserted {
			t, err := hex.DecodeString(msg.Key.String())
			if err != nil {
				p.err(err.Error())
				return false
			}
			err = p.Subscribe(Topic(t))
			if err != nil {
				p.err(err.Error())
				return false
			}
			// Prevent the message from continuing
			return false
		}
		break
	case MSG_UNSUBSCRIBE:
		removed, empty := p.subscriptions.remove(val, msg.Sender.ID)
		if removed {
			if empty {
				t, err := hex.DecodeString(msg.Key.String())
				if err != nil {
					p.err(err.Error())
					return false
				}
				err = p.Unsubscribe(Topic(t))
				if err != nil {
					p.err(err.Error())
					return false
				}
			}
			// Prevent the message from continuing
			return false
		}
		break
	case MSG_EVENT:
		p.notifySubscribers(val)
		break
	}
	return true
}

// OnNewLeaves fulfills the wendy.Application interface. It will be called when Wendy's leaf set is updated. It is a stub.
func (p *Peter) OnNewLeaves(leafset []*wendy.Node) {
}

// OnNodeJoin fulfills the wendy.Application interface, and will be called whenever a Node leaves the Cluster. Peter will detect which topics would use that Node as a parent in the subscription tree, and re-subscribe to those topics to repair the subscription tree.
func (p *Peter) OnNodeJoin(node wendy.Node) {
	// TODO: call wendy.Route on each topic, and if it returns the joined Node, resubscribe
}

// OnNodeExit fulfills the wendy.Application interface, and will be called whenever a Node leaves the Cluster. Peter will re-subscribe to any topics that Node was responsible for updating the current Node about, repairing the subscription tree.
func (p *Peter) OnNodeExit(node wendy.Node) {
	topics := p.parents.list(node.ID)
	for _, topic := range topics {
		err := p.Subscribe(topic)
		if err != nil {
			p.err(err.Error())
		}
	}
	p.subscriptions.removeSubscriber(node.ID)
}

// OnHeartbeat exists only to fulfill the wendy.Application interface. It will be called whenever a heartbeat is received in Wendy, checking for Node health. It is a stub.
func (p *Peter) OnHeartbeat(node wendy.Node) {
}

func (p *Peter) notifySubscribers(t Topic) error {
	// TODO: fan out notification to each subscriber
	return nil
}

// SetLogger sets the log.Logger that Peter and its underlying cluster will write to.
func (p *Peter) SetLogger(l *log.Logger) {
	p.log = l
}

// SetLogLevel sets the level of logging that will be written to the Logger. It will be mirrored to the cluster.
//
// Use peter.LogLevelDebug to write to the most verbose level of logging, helpful for debugging.
//
// Use peter.LogLevelWarn (the default) to write on events that may, but do not necessarily, indicate an error.
//
// Use peter.LogLevelError to write only when an event occurs that is undoubtedly an error.
func (p *Peter) SetLogLevel(level int) {
	p.logLevel = level
	switch level {
	case LogLevelDebug:
		p.cluster.SetLogLevel(wendy.LogLevelDebug)
	case LogLevelWarn:
		p.cluster.SetLogLevel(wendy.LogLevelWarn)
	case LogLevelError:
		p.cluster.SetLogLevel(wendy.LogLevelError)
	}
}

func (p *Peter) debug(format string, v ...interface{}) {
	if p.logLevel <= LogLevelDebug {
		p.log.Printf(format, v...)
	}
}

func (p *Peter) warn(format string, v ...interface{}) {
	if p.logLevel <= LogLevelWarn {
		p.log.Printf(format, v...)
	}
}

func (p *Peter) err(format string, v ...interface{}) {
	if p.logLevel <= LogLevelError {
		p.log.Printf(format, v...)
	}
}
