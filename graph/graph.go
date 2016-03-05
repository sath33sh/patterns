// This package implements a directed graph which supports many kinds of relationships
// between nodes.
package graph

import (
	"fmt"
	"github.com/sath33sh/infra/db"
	"github.com/sath33sh/infra/log"
	"strconv"
	"time"
)

// Module name.
const MODULE = "graph"

// Graph node ID.
type NodeId string

// Node.
// NOTE: Node structure is not independently stored in the database.
// It is designed to be embedded in other parent structures.
type Node struct {
	Type  db.ObjType `json:"type,omitempty"`  // Type of node. Also happens to be db object type.
	Id    NodeId     `json:"id,omitempty"`    // Node ID.
	Name  string     `json:"name,omitempty"`  // Full name.
	Photo string     `json:"photo,omitempty"` // Thumbnail photo URL.
}

// Relationship verb.
type RelationVerb string

// Some predefined relation verbs.
const (
	FOLLOW   RelationVerb = "follow"   // Follow.
	VIEW                  = "view"     // View.
	LIKE                  = "like"     // Like.
	FRIEND                = "friend"   // Friend.
	CONTAIN               = "contain"  // Contain.
	OWN                   = "own"      // Own.
	MODERATE              = "moderate" // Moderate.
	FEDERATE              = "federate" // Federate.
	INVITE                = "invite"   // Invite.
	CONNECT               = "connect"  // Connect.
	IGNORE                = "ignore"   // Ignore.
	BLOCK                 = "block"    // Block.
)

// Relationship map.
type Relation map[RelationVerb]bool

// Options.
type Options struct {
	Ord int `json:"ord,omitempty"` // Ordinal. Can be used to order arcs.
}

// Arc object type.
const OBJ_ARC db.ObjType = "arc"

// Arc.
type Arc struct {
	Type      db.ObjType `json:"type"`      // Document type: "arc"
	Tail      Node       `json:"tail"`      // Tail of the arc.
	Head      Node       `json:"head"`      // Head of the arc.
	Relation  Relation   `json:"relation"`  // Relationship map.
	Options   Options    `json:"options"`   // Options.
	CreatedAt time.Time  `json:"createdAt"` // Arc creation time.
}

// Allocate a new node ID.
func NewNodeId() (NodeId, error) {
	// Auto Increment.
	idNum, err := db.Buckets[db.DEFAULT_BUCKET].Counter("id:node", 1, 1, 0)

	return NodeId(strconv.FormatUint(uint64(idNum), 10)), err
}

// Document interface methods for arc.
func (a *Arc) GetMeta() db.ObjMeta {
	return db.ObjMeta{
		Bucket: db.DEFAULT_BUCKET,
		Type:   OBJ_ARC,
		Id:     fmt.Sprintf("%s:%s>%s:%s", a.Tail.Type, a.Tail.Id, a.Head.Type, a.Head.Id),
	}
}

func (a *Arc) SetType() {
	a.Type = OBJ_ARC
}

/*
// Arc event.
type ArcEvent struct {
	Tail     Node     `json:"tail,omitempty"`     // Tail.
	Head     Node     `json:"head,omitempty"`     // Head.
	Relation Relation `json:"relation,omitempty"` // Relation.
	From     Node     `json:"from,omitempty"`     // Event originator.
}

// Event interface method.
func (ev *ArcEvent) EncodeEventPayload() (event.CategoryIndex, *event.Payload, error) {
	p := &event.Payload{
		Key:  string(ev.Head.Id),
		Type: OBJ_ARC,
	}

	// Encode data.
	var err error
	if p.Data, err = json.Marshal(ev); err != nil {
		log.Errorf("Event payload JSON marshal error: %v", err)
		return event.GRAPH_CATEGORY, nil, util.ERR_JSON_DECODE
	}

	return event.GRAPH_CATEGORY, p, nil
}
*/

// Create arc between two nodes.
func CreateArc(tail, head *Node, r *Relation, opts Options) (err error) {
	var a Arc

	a.Tail = *tail
	a.Head = *head
	a.Relation = *r
	a.Options = opts
	a.CreatedAt = time.Now()

	if err = db.Upsert(&a, 0); err != nil {
		return err
	}

	return err
}

// Get all relationships between two nodes.
func GetRelation(tail, head *Node) (r Relation, err error) {
	a := Arc{Tail: Node{Type: tail.Type, Id: tail.Id}, Head: Node{Type: head.Type, Id: head.Id}}
	if err = db.Get(&a); err == nil {
		r = a.Relation
	}

	return r, err
}

// Update relationship between two actors.
func UpdateRelation(tail, head *Node, r *Relation) (err error) {
	// Perform RWM.

	// Read.
	var lock db.Lock
	la := Arc{Tail: Node{Type: tail.Type, Id: tail.Id}, Head: Node{Type: head.Type, Id: head.Id}}
	if lock, err = db.GetLock(&la); err != nil {
		return err
	}

	// Update.
	la.Tail = *tail
	la.Head = *head

	// Merge in new relation.
	for key, val := range *r {
		if val {
			la.Relation[key] = val
		} else {
			delete(la.Relation, key)
		}
	}

	// Write.
	if err = db.WriteUnlock(&la, lock, 0); err != nil {
		return err
	}

	return err
}

// Query result which contains list of node.
type NodeQueryResult struct {
	Results    []Node `json:"results,omitempty"` // Results is list of nodes.
	NextOffset string `json:"nextOffset"`        // Next offset.
	PrevOffset string `json:"prevOffset"`        // Previous offset.
}

func (qr *NodeQueryResult) GetRowPtr(index int) interface{} {
	if index < len(qr.Results) {
		return &qr.Results[index]
	} else if index == len(qr.Results) {
		qr.Results = append(qr.Results, Node{})
		return &qr.Results[index]
	} else {
		return nil
	}
}

// Query list of tail actors given a head actor ID and relation.
func (qr *NodeQueryResult) QueryTails(head *Node, r RelationVerb, limit, offset int) (size int, err error) {
	// N1QL query statement.
	queryStmt := fmt.Sprintf("SELECT tail.* FROM `%s` WHERE type=\"%s\" AND head.type=\"%s\" AND head.id=\"%s\" AND relation.%s=true ORDER BY tail.name",
		db.BucketName(db.DEFAULT_BUCKET), OBJ_ARC, head.Type, head.Id, r)

	size, err = db.ExecPagedQuery(db.DEFAULT_BUCKET, qr, queryStmt, limit, offset)
	if err != nil {
		return size, err
	}

	qr.Results = qr.Results[:size]
	qr.PrevOffset = fmt.Sprintf("%d", offset)
	qr.NextOffset = fmt.Sprintf("%d", offset+size)

	return size, err
}

// Query list of head actors given a tail actor ID and relation.
func (qr *NodeQueryResult) QueryHeads(tail *Node, r RelationVerb, limit, offset int) (size int, err error) {
	// N1QL query statement.
	queryStmt := fmt.Sprintf("SELECT head.* FROM `%s` WHERE type=\"%s\" AND tail.type=\"%s\" AND tail.id=\"%s\" AND relation.%s=true ORDER BY head.name",
		db.BucketName(db.DEFAULT_BUCKET), OBJ_ARC, tail.Type, tail.Id, r)

	size, err = db.ExecPagedQuery(db.DEFAULT_BUCKET, qr, queryStmt, limit, offset)
	if err != nil {
		return size, err
	}

	qr.Results = qr.Results[:size]
	qr.PrevOffset = fmt.Sprintf("%d", offset)
	qr.NextOffset = fmt.Sprintf("%d", offset+size)

	return size, err
}

// Count tails to a head.
func Indegree(head *Node, r RelationVerb) (count int, err error) {
	// N1QL query statement.
	queryStmt := fmt.Sprintf("SELECT COUNT(*) FROM `%s` WHERE `type`=\"%s\" AND head.type=\"%s\" AND head.id=\"%s\" AND relation.%s=true",
		db.BucketName(db.DEFAULT_BUCKET), OBJ_ARC, head.Type, head.Id, r)

	count, err = db.ExecCount(db.DEFAULT_BUCKET, queryStmt)
	if err != nil {
		log.Errorf("Failed to count tails of %s:%s: %v", head.Type, head.Id, err)
		return 0, err
	}

	return count, nil
}

// Count heads from a tail.
func Outdegree(tail *Node, r RelationVerb) (count int, err error) {
	// N1QL query statement.
	queryStmt := fmt.Sprintf("SELECT COUNT(*) FROM `%s` WHERE `type`=\"%s\" AND tail.type=\"%s\" AND tail.id=\"%s\" AND relation.%s=true",
		db.BucketName(db.DEFAULT_BUCKET), OBJ_ARC, tail.Type, tail.Id, r)

	count, err = db.ExecCount(db.DEFAULT_BUCKET, queryStmt)
	if err != nil {
		log.Errorf("Failed to count heads of %s:%s: %v", tail.Type, tail.Id, err)
		return 0, err
	}

	return count, nil
}

// Iterator for tails.
func ForEachTail(head *Node, r RelationVerb, cb func(*Node)) {
	var err error

	// N1QL query statement.
	queryStmt := fmt.Sprintf("SELECT tail.* FROM `%s` WHERE type=\"%s\" AND head.type=\"%s\" AND head.id=\"%s\" AND relation.%s=true",
		db.BucketName(db.DEFAULT_BUCKET), OBJ_ARC, head.Type, head.Id, r)

	size := db.QUERY_LIMIT_MAX
	offset := 0
	for size == db.QUERY_LIMIT_MAX {
		var qr NodeQueryResult
		size, err = db.ExecPagedQuery(db.DEFAULT_BUCKET, &qr, queryStmt, size, offset)
		if err != nil {
			return
		}

		for index := 0; index < size; index++ {
			cb(&qr.Results[index])
		}
		offset += size
	}
}

// Iterator for heads.
func ForEachHead(tail *Node, r RelationVerb, cb func(*Node)) {
	var err error

	// N1QL query statement.
	queryStmt := fmt.Sprintf("SELECT head.* FROM `%s` WHERE type=\"%s\" AND tail.type=\"%s\" AND tail.id=\"%s\" AND relation.%s=true",
		db.BucketName(db.DEFAULT_BUCKET), OBJ_ARC, tail.Type, tail.Id, r)

	size := db.QUERY_LIMIT_MAX
	offset := 0
	for size == db.QUERY_LIMIT_MAX {
		var qr NodeQueryResult
		size, err = db.ExecPagedQuery(db.DEFAULT_BUCKET, &qr, queryStmt, size, offset)
		if err != nil {
			return
		}

		for index := 0; index < size; index++ {
			cb(&qr.Results[index])
		}
		offset += size
	}
}
