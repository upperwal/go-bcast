package main

/* Binary tree based Broadcast
*  This program demonstrate a tree based broadcast to all the peers in a network.
*  Program starts by arranging a set of peers in a tree structure were a node is
*  connected to exact two nodes which is called it's children. This gives an almost
*  complete binary tree.
*
*  One-way flow of information:
*  Any data can only flow from top to bottom of the graph. So, if the root node initiates
*  a broadcast. It will send the message to it's children who will then send it to their
*  children. This way a message can reach all the nodes with O(log n) steps in the network.
*
*  What if the message is originated from a non-root node?
*  As the flow of messages is from top to bottom, root node might never get a message if the
*  broadcast is originated from one of the non-root node. It is handled by connecting all the nodes
*  in the last level of the tree with in a binary tree manner and the last node in the tree with the
*  root node such that it now becomes a cyclic graph.
*
*  Connecting last level of the tree (actually a graph) in it's own binary tree for speed up.
*  As last level of a tree contains O(n/2) nodes if all the nodes are connected in a linear manner
*  it will take O(n/2) steps to get to the last node which will then send the message to the root
*  node and start the binary broadcast. What if last level nodes are connected in a binary tree
*  of it's own. Now a message from the left most nodes in the last level can reach the rightmost
*  node in the last level in O(log n/2) steps. This will speed-up the broadcast of the broadcast is
*  originated from any of the leaf node.
 */

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/upperwal/go-bcast/protocol"
)

const (
	portStart = 3000
)

// ProtoWConn encapsulates incoming connection with the packet.
type ProtoWConn struct {
	Conn *net.Conn
	Data *protocol.Protocol
}

// Node represents a host.
type Node struct {
	rank               int
	universeSize       int
	connToPeer         [2]*net.Conn
	neighbours         [2]int
	outgoing           chan *protocol.Protocol
	incoming           chan *ProtoWConn
	outgoingControlRes chan *ProtoWConn
	dupMessage         map[[32]byte]bool // Could have a time bound.
	mux                *sync.Mutex
}

// NewNode creates a new host.
func NewNode(r, u int) *Node {

	n := &Node{
		rank:               r,
		universeSize:       u,
		outgoing:           make(chan *protocol.Protocol, 10),
		incoming:           make(chan *ProtoWConn, 10),
		outgoingControlRes: make(chan *ProtoWConn, 10),
		dupMessage:         make(map[[32]byte]bool),
		mux:                &sync.Mutex{},
	}

	n.setupSocket()
	go n.messageLoop()

	return n
}

// Start connects to the peers.
func (n *Node) Start() {
	n.connectToPeer()
}

func (n *Node) setupSocket() {
	l, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(n.rank+portStart))
	handleErr(err)

	go n.listen(l)
}

func (n *Node) connectToPeer() {
	var vitrualStartNumber, peerOne, peerTwo int

	// Which node to connect to?
	// Each node will be connected to exactly two nodes. Non-leaf nodes will be connected to it's children.
	// If any non-leaf node only have 1 child (incase of almost complete binary tree) the second connection
	// is established with some node in the middle. This will improve the overall runtime in some cases.
	//
	// All leaf-nodes are connected in a binary tree which is independent of the binary tree of non-leaf
	// nodes. Leaf nodes binary tree will speedup the time it takes to propagate a message from left-most
	// to right-most node in the last level.
	completeTree := int(math.Pow(2, (math.Ceil(math.Log2(float64(n.universeSize)))-1)) - 1)

	if n.rank >= completeTree {
		vitrualStartNumber = completeTree
	} else {
		vitrualStartNumber = 0
	}

	virtualNodeNumber := n.rank - vitrualStartNumber

	if n.rank == n.universeSize-1 {
		peerOne = 0
		peerTwo = vitrualStartNumber
	} else {
		peerOne = (2*virtualNodeNumber + 1 + vitrualStartNumber) % n.universeSize
		peerTwo = (peerOne + 1) % n.universeSize
	}

	n.neighbours[0] = peerOne
	n.neighbours[1] = peerTwo

	fmt.Println("Node ", n.rank, " >> ", peerOne, ", ", peerTwo)

	connPeerOne, err := net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(portStart+peerOne))
	handleErr(err)
	n.connToPeer[0] = &connPeerOne
	go n.handler(&connPeerOne)

	connPeerTwo, err := net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(portStart+peerTwo))
	handleErr(err)
	n.connToPeer[1] = &connPeerTwo
	go n.handler(&connPeerTwo)
}

func (n *Node) messageLoop() {
	for {
		select {
		case o := <-n.outgoing:
			//fmt.Println("Outgoing ", o.Type)
			connPeerOne := *n.connToPeer[0]
			connPeerTwo := *n.connToPeer[1]
			data, err := proto.Marshal(o)
			handleErr(err)

			n.write(connPeerOne, data)
			n.write(connPeerTwo, data)

		case i := <-n.incoming:
			//fmt.Println("Incoming ", i.Data.Type)

			if i.Data.Type == protocol.Protocol_DATA_PACKET {
				go n.processDataPacket(i)
			} else if i.Data.Type == protocol.Protocol_CONTROL_REQ {
				go n.processControlPacket(i)
			} else if i.Data.Type == protocol.Protocol_CONTROL_RES {
				go n.processControlRes(i)
			}
		case o := <-n.outgoingControlRes:
			data, err := proto.Marshal(o.Data)
			handleErr(err)

			conn := *o.Conn

			n.write(conn, data)
		}
	}
}

func (n *Node) write(conn net.Conn, data []byte) {
	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, uint64(len(data)))
	conn.Write(bs)
	conn.Write(data)
}

func (n *Node) processDataPacket(i *ProtoWConn) {
	switch i.Data.DataPacket.Type {
	case protocol.Protocol_DataPacket_BROADCAST:
		fmt.Println("Incoming Broadcast: [This Peer: "+strconv.Itoa(n.rank)+"] Data: "+i.Data.DataPacket.Data, " | Forwarding to: ", n.neighbours[0], " / ", n.neighbours[1])
		// Read the data and send the packet as it is to your children.
		n.outgoing <- i.Data
	}
}

func (n *Node) processControlPacket(i *ProtoWConn) {
	switch i.Data.ControlReq.Type {
	case protocol.Protocol_ControlReq_GET_NEIGHBOURS:
		fmt.Println("Received GET_NEIGHBOUR request")
		res := &protocol.Protocol{
			Type: protocol.Protocol_CONTROL_RES,
			ControlRes: &protocol.Protocol_ControlRes{
				From: int64(n.rank),
				Data: strconv.Itoa(n.neighbours[0]) + ", " + strconv.Itoa(n.neighbours[1]),
			},
		}

		n.outgoingControlRes <- &ProtoWConn{
			Conn: i.Conn,
			Data: res,
		}
	}
}

func (n *Node) processControlRes(i *ProtoWConn) {
	fmt.Println("GET_NEIGHBOUR Response Neighbours: [Peer ", i.Data.ControlRes.From, "]: ", i.Data.ControlRes.Data)
}

func (n *Node) listen(l net.Listener) {
	defer l.Close()
	for {
		conn, err := l.Accept()
		handleErr(err)

		go n.handler(&conn)
	}
}

func (n *Node) handler(conn *net.Conn) {
	defer (*conn).Close()
	p := &protocol.Protocol{}

	for {
		bs := make([]byte, 8)
		io.ReadFull(*conn, bs)
		s := int(binary.LittleEndian.Uint64(bs))

		buf := make([]byte, s)
		io.ReadFull(*conn, buf)

		err := proto.Unmarshal(buf, p)
		handleErr(err)

		hash := sha256.Sum256(buf)
		if n.dupMessage[hash] && p.Type == protocol.Protocol_DATA_PACKET {
			continue
		}

		n.mux.Lock()
		n.dupMessage[hash] = true
		n.mux.Unlock()

		n.incoming <- &ProtoWConn{
			Conn: conn,
			Data: p,
		}
	}

}

func handleErr(err error) {
	if err != nil {
		fmt.Println("Error: ", err)
	}
}

func (n *Node) getNeighboursOfNeighbour() {
	n.outgoing <- &protocol.Protocol{
		Type: protocol.Protocol_CONTROL_REQ,
		ControlReq: &protocol.Protocol_ControlReq{
			Type: protocol.Protocol_ControlReq_GET_NEIGHBOURS,
		},
	}
}

// Broadcast sends data to all the peers with log n messages in the network.
func (n *Node) Broadcast(msg string) {
	n.outgoing <- &protocol.Protocol{
		Type: protocol.Protocol_DATA_PACKET,
		DataPacket: &protocol.Protocol_DataPacket{
			Type: protocol.Protocol_DataPacket_BROADCAST,
			Data: msg,
		},
	}
}

func main() {
	nList := make([]*Node, 0)
	universe := 15 // universe size

	for i := 0; i < universe; i++ {
		n := NewNode(i, universe)
		nList = append(nList, n)
	}

	for _, n := range nList {
		n.Start()
	}

	bcastInitPeer := 2   // This peer will start the broadcast.
	bcastdata := "hello" // Broadcast message.
	fmt.Println("Broadcast Initiated by: peer ", bcastInitPeer, " with data: ", bcastdata)
	nList[bcastInitPeer].Broadcast(bcastdata)

	time.Sleep(2 * time.Second)

	// Node can also handle control signals or messages like quering connect peers.
	fmt.Println("Sending GET_NEIGHBOUR request")
	nList[bcastInitPeer].getNeighboursOfNeighbour()

	time.Sleep(4 * time.Second)
}
