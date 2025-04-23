package kinesisfake

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"

	kinesistypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"reduction.dev/reduction/util/ptr"
)

func StartFake() (*httptest.Server, *Fake) {
	db := &db{
		streams: make(map[string]*stream),
	}
	mux := http.NewServeMux()
	fk := &Fake{db: db}

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		route(fk, w, r)
	})

	return httptest.NewServer(mux), fk
}

func route(f *Fake, w http.ResponseWriter, r *http.Request) {
	operation := strings.Split(r.Header.Get("x-amz-target"), ".")[1]
	w.Header().Set("Content-Type", "application/json")

	body, err := io.ReadAll(r.Body)
	if err != nil {
		handleError(w, err)
		return
	}
	slog.Info("routing", "op", operation, "req", body)

	var resp any
	switch operation {
	case "DescribeStream":
		resp, err = f.describeStream(body)
	case "CreateStream":
		resp, err = f.createStream(body)
	case "PutRecords":
		resp, err = f.putRecords(body)
	case "ListShards":
		resp, err = f.listShards(body)
	case "GetShardIterator":
		resp, err = f.getShardIterator(body)
	case "GetRecords":
		resp, err = f.getRecords(body)
	case "DeleteStream":
		resp, err = f.deleteStream(body)
	case "MergeShards":
		resp, err = f.mergeShards(body)
	default:
		err = &kinesistypes.InvalidArgumentException{Message: ptr.New(fmt.Sprintf("Invalid Operation: %s", operation))}
	}

	if err != nil {
		handleError(w, err)
		return
	}

	slog.Info("resp", "resp", resp)
	json.NewEncoder(w).Encode(resp)
}

type db struct {
	streams map[string]*stream
}

type stream struct {
	shards []*shard
}

// Interal tracked state of a shard
type shard struct {
	id                  string
	records             []Record
	hashKeyRange        hashKeyRange
	isFinished          bool
	parentShard         string
	adjacentParentShard string
}

type hashKeyRange struct {
	startingHashKey *big.Int
	endingHashKey   *big.Int
}

func (r hashKeyRange) includes(key *big.Int) bool {
	return r.startingHashKey.Cmp(key) <= 0 && r.endingHashKey.Cmp(key) >= 0
}

type Fake struct {
	db                    *db
	lastIteratorTimestamp atomic.Int64
	iteratorsExpirationAt atomic.Int64
	getRecordsError       error
}
