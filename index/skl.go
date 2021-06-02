package index

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	maxLevel int = 10
)

type (
	// SkipList 跳跃表
	SkipList struct {
		header *Node
		tail *Node
		size int
	}
	// Node 跳跃表节点
	Node struct {
		obj *element
		level []*Node
	}

	// 节点数据
	element struct {
		key []byte
		val interface{}
	}
)

// Value 返回值信息
func (e *element) Value() interface{} {
	return e.val
}

// InitSkl 建立跳跃表
func InitSkl() *SkipList {
	head := &Node{
		nil,
		make([]*Node, maxLevel),
	}
	return &SkipList{header: head, tail: nil, size: 0}
}

// Find 查询节点信息
func (sk *SkipList) Find(key []byte) *element {
	node := sk.header
	for i := maxLevel-1; i >= 0; i-- {
		for node.level[i] != nil && string(node.level[i].obj.key) < string(key) {
			node = node.level[i]
		}
	}
	node = node.level[0]
	if node != nil && string(node.obj.key) == string(key) {
		return node.obj
	}
	return nil
}

// Insert 插入节点
func (sk *SkipList) Insert(key []byte, value interface{}) bool {
	node := sk.header
	update := make([]*Node, maxLevel)

	// 查找插入节点的前驱节点
	for i := maxLevel-1; i >= 0; i-- {
		for node.level[i] != nil && string(node.level[i].obj.key) < string(key) {
			node = node.level[i]
		}
		update[i] = node
	}

	// 如果插入节点已经存在, 更新节点信息
	if node.level[0] != nil && string(node.level[0].obj.key) == string(key) {
		node.level[0].obj.val = value
		return false
	}

	// 长度加一
	sk.size++
	// 获取节点层数
	nodeLevel := getRandomLevel()

	// 新建插入节点
	newNode := &Node{&element{key: key, val: value}, make([]*Node, nodeLevel)}

	// 修改前驱指针
	for i := nodeLevel-1; i >= 0; i-- {
		node = update[i]
		newNode.level[i] = node.level[i]
		node.level[i] = newNode
	}

	// 返回
	return true
}

// Traverse 遍历跳跃表
func (sk *SkipList) Traverse() {
	node := sk.header.level[0]
	for node != nil {
		fmt.Printf("%s ", node.obj.key)
		node = node.level[0]
	}
	fmt.Println()
}

// Remove 删除节点
func (sk *SkipList) Remove(key []byte) *element {
	node := sk.header
	update := make([]*Node, maxLevel)

	// 查找待删除节点的前驱节点
	for i := maxLevel-1; i >= 0; i-- {
		for node.level[i] != nil && string(node.level[i].obj.key) < string(key) {
			node = node.level[i]
		}
		update[i] = node
	}

	// 删除节点不存在，返回false
	if node.level[0] == nil || string(node.level[0].obj.key) != string(key) {
		return nil
	}

	// node为要删除的节点
	node = node.level[0]

	// 删除node, 更新level指针的指向
	for i := maxLevel-1; i >= 0; i-- {
		if update[i].level[i] == node {
			update[i].level[i] = node.level[i]
		}
	}

	// 长度减一
	sk.size--

	// 返回结果
	// fmt.Printf("%x", node.obj.key)
	return node.obj
}

// 获取随机层数
func getRandomLevel() int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(maxLevel-1)+1
}

//func main() {
//	skl := InitSkl()
//	for i := 0; i < 100; i++ {
//		s := strconv.Itoa(i)
//		skl.Insert(s, s)
//	}
//
//	for i := 0; i < 30; i++ {
//		str := strconv.Itoa(rand.Intn(100))
//		fmt.Printf("%s ", str)
//		skl.Remove(str)
//	}
//	fmt.Println()
//
//	skl.Traverse()
//}
