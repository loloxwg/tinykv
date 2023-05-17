package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.GetCF
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawGetResponse{Value: value, NotFound: value == nil}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified

	batch := storage.Modify{
		Data: storage.Put{Cf: req.Cf, Key: req.Key, Value: req.Value},
	}

	err := server.storage.Write(req.Context, []storage.Modify{batch})
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	batch := storage.Delete{
		Cf:  req.Cf,
		Key: req.Key,
	}
	err := server.storage.Write(req.Context, []storage.Modify{{Data: batch}})
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	iter := reader.IterCF(req.Cf)
	defer iter.Close()
	iter.Seek(req.StartKey)
	var kvs []*kvrpcpb.KvPair
	for i := uint32(0); i < req.Limit; i++ {
		if !iter.Valid() {
			break
		}
		item := iter.Item()
		value, err := item.Value()
		if err != nil {
			return nil, err
		}
		kvs = append(kvs, &kvrpcpb.KvPair{Key: item.Key(), Value: value})
		iter.Next()
	}
	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil
}
