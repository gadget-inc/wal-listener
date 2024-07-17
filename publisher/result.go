package publisher

import "context"

// A PublishResult holds the result from a call to Publish.
type PublishResult interface {
	Ready() <-chan struct{}
	Get(ctx context.Context) (serverID string, err error)
}

// A permanently successful PublishResult
type SuccessResult struct {
	serverID string
	ready    chan struct{}
}

// A permanently failed PublishResult
type ErrorResult struct {
	err   error
	ready chan struct{}
}

func (r *SuccessResult) Ready() <-chan struct{} {
	return r.ready
}

func (r *SuccessResult) Get(ctx context.Context) (serverID string, err error) {
	return r.serverID, nil
}

func NewSuccessResult(serverID string) *SuccessResult {
	return &SuccessResult{
		serverID: serverID,
		ready:    make(chan struct{}),
	}
}

func (r *ErrorResult) Ready() <-chan struct{} {
	return r.ready
}

func (r *ErrorResult) Get(ctx context.Context) (serverID string, err error) {
	return "", r.err
}

func NewErrorResult(err error) *ErrorResult {
	return &ErrorResult{
		err:   err,
		ready: make(chan struct{}),
	}
}

func Result(err error) PublishResult {
	if err == nil {
		return NewSuccessResult("1")
	}
	return NewErrorResult(err)
}
