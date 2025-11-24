package s3lect

import (
	"context"
	"sync"
)

// SpyStorage wraps MockStorage to capture calls
type SpyStorage struct {
	*MockStorage
	mu              sync.Mutex
	putIfMatchCalls []PutIfMatchCall
}

type PutIfMatchCall struct {
	Key  string
	ETag string
}

func (s *SpyStorage) PutIfMatch(ctx context.Context, key string, data []byte, etag string) error {
	s.mu.Lock()
	s.putIfMatchCalls = append(s.putIfMatchCalls, PutIfMatchCall{Key: key, ETag: etag})
	s.mu.Unlock()
	return s.MockStorage.PutIfMatch(ctx, key, data, etag)
}
