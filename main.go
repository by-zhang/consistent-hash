package main

import (
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const DEFAULT_REPLICAS_NUM = 6

type ConsistentHash struct {
	Nodes       map[uint32]*Node     /** node的hash值 => node 对象        **/
	ReplicasNum int                  /** 每一个node生成ReplicasNum个vnode **/
	Resources   map[int]bool         /** nodeid集合                       **/
	ring        HashRing             /** hash环 uint32数组                **/
	sync.RWMutex
}

type HashRing []uint32

func (hr HashRing) Len() int {
	return len(hr)
}

func (hr HashRing) Less(i, j int) bool {
	return hr[i] < hr[j]
}

func (hr HashRing) Swap(i, j int) {
	hr[i], hr[j] = hr[j], hr[i]
}

type Node struct {
	Id       int
	Ip       string
	Port     int
	HostName string
	Weight   int
}

func getInstance() *ConsistentHash {
	return &ConsistentHash{
		Nodes:       make(map[uint32]*Node),
		ReplicasNum: DEFAULT_REPLICAS_NUM,
		Resources:   make(map[int]bool),
		ring:        HashRing{},
	}
}

func NewNode(id int, ip string, port int, name string, weight int) *Node {
	return &Node{
		Id:       id,
		Ip:       ip,
		Port:     port,
		HostName: name,
		Weight:   weight,
	}
}

/** 添加一个node **/
func (ch *ConsistentHash) AddNode(node *Node) error {
	ch.Lock()
	defer ch.Unlock()
	if _, flag := ch.Resources[node.Id]; flag {
		return errors.New("add node failed: node id has already been registered.")
	}
	ch.Resources[node.Id] = true
	vNodeCount := node.Weight * ch.ReplicasNum
	for i := 0; i < vNodeCount; i++ {
		ch.Nodes[ch.Hash(node, i)] = node
	}
	ch.sort()
	return nil
}

/** 移除一个node **/
func (ch *ConsistentHash) RemoveNode(node *Node) error {
	ch.Lock()
	defer ch.Unlock()
	if _, flag := ch.Resources[node.Id]; !flag {
		return errors.New("remove node failed: node id is not existed.")
	}
	delete(ch.Resources, node.Id)
	vNodeCount := node.Weight * ch.ReplicasNum
	for i := 0; i < vNodeCount; i++ {
		delete(ch.Nodes, ch.Hash(node, i))
	}
	ch.sort()
	return nil
}

/** 求node的hash值 **/
func (ch *ConsistentHash) Hash(node *Node, index int) uint32 {
	tempSlice := []string{node.Ip, strconv.Itoa(node.Weight), strconv.Itoa(index), strconv.Itoa(node.Id)}
	str := strings.Join(tempSlice, "-")
	return crc32.ChecksumIEEE([]byte(str))
}

/** 获取存储对象的node **/
func (ch *ConsistentHash) Get(key string) Node {
	ch.RLock()
	defer ch.RUnlock()
	hash := crc32.ChecksumIEEE([]byte(key))
	index := ch.search(hash)
	return *ch.Nodes[ch.ring[index]]
}

/** 排序hash环 **/
func (ch *ConsistentHash) sort() {
	ch.ring = HashRing{}
	for key, _ := range ch.Nodes {
		ch.ring = append(ch.ring, key)
	}
	sort.Sort(ch.ring)
}

/** 对象hash查询存储位置node的hash **/
func (ch *ConsistentHash) search(hashval uint32) uint32 {
	index := sort.Search(len(ch.ring), func(i int) bool {
		return ch.ring[i] >= hashval
	})
	if index < len(ch.ring) {
		return uint32(index)
	} else {
		return 0
	}
}

func main() {
	ins := getInstance()
	node1 := NewNode(0, "172.168.0.1", 8080, "host001", 1)
	node2 := NewNode(1, "172.168.0.5", 8080, "host002", 1)
	node3 := NewNode(3, "172.168.0.10", 8080, "host003", 1)
	node4 := NewNode(4, "172.168.0.11", 8081, "host004", 1)
	err := ins.AddNode(node1)
	err = ins.AddNode(node2)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(show(*ins))
	err = ins.RemoveNode(node2)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(show(*ins))
	ins.AddNode(node2)
	fmt.Println(show(*ins))
	ins.AddNode(node3)
	fmt.Println(show(*ins))
	ins.AddNode(node4)
	fmt.Println(show(*ins))
}

func show(ins ConsistentHash) map[string]int {
	testMap := map[string]int{}
	for i := 0; i < 50000; i++ {
		node := ins.Get("key#" + strconv.Itoa(i))
		if _, flag := testMap[node.Ip]; flag {
			testMap[node.Ip]++
		} else {
			testMap[node.Ip] = 1
		}
	}
	return testMap
}
