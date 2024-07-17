package listener

import (
	"runtime"
	"sync"
)

type pipe struct {
	in            jobChan
	out           jobChan
	processErrors bool

	queue     chan queuedJobChan
	closeInCh chan struct{}
}

// Job is a short callback signature, used in pipes
type Job func(msg interface{}) interface{}

type (
	jobChan       chan interface{}
	queuedJobChan chan interface{}
)

func newPipe(job Job, concurrency int, processErrors bool) *pipe {
	p := &pipe{
		in:            make(jobChan, 1),
		out:           make(jobChan, 1),
		processErrors: processErrors,

		queue:     make(chan queuedJobChan, concurrency),
		closeInCh: make(chan struct{}),
	}

	// Create worker pool
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for msg := range p.in {
				queued := make(queuedJobChan, 1)
				_, isError := msg.(error)
				if isError && !p.processErrors {
					queued <- msg
				} else {
					queued <- job(msg)
				}
				p.queue <- queued
			}
		}()
	}

	go func() {
		// Wait for all workers to finish
		wg.Wait()
		// Close the queue channel after all messages have been processed
		close(p.queue)
	}()

	go func() {
		for processed := range p.queue {
			p.out <- <-processed
			close(processed)
		}
		close(p.out)
	}()

	return p
}

// Pipeline executes jobs concurrently maintaining message order
type Pipeline struct {
	cfg    Config
	pipes  []*pipe
	hasOut bool
	closed bool
}

// Config contains pipeline parameters which influence execution or behavior
type Config struct {
	ProcessErrors bool // if false, messages implementing "error" interface will not be passed to subsequent workers
}

// NewPipeline creates new pipeline instance, "Concurrency" sets how many jobs can be executed concurrently in each pipe
func NewPipeline(cfg Config) *Pipeline {
	return &Pipeline{
		pipes: make([]*pipe, 0, 1),
		cfg:   cfg,
	}
}

// Push adds a value to the pipeline for processing, it is immediately queued to be processed
func (p *Pipeline) Push(v interface{}) {
	p.pipes[0].in <- v
}

// Pipe adds new pipe to pipeline with the callback for processing each message
// Concurrency indicates how many messages to process concurrently for this pipe
func (p *Pipeline) Pipe(concurrency int, job Job) *Pipeline {
	if concurrency < 1 {
		concurrency = runtime.NumCPU()
	}

	if p.hasOut || p.closed {
		panic("attempt to create new pipeline after Out() call")
	}

	pipe := newPipe(job, concurrency, p.cfg.ProcessErrors)

	if len(p.pipes) > 0 {
		bindChannels(p.pipes[len(p.pipes)-1].out, pipe.in)
	}

	p.pipes = append(p.pipes, pipe)

	return p
}

// Out returns exit of the pipeline - channel with results of the last pipe. Call it once - it is not idempotent!
func (p *Pipeline) Out() <-chan interface{} {
	p.hasOut = true
	return p.pipes[len(p.pipes)-1].out
}

// Close closes pipeline input channel, from that moment pipeline processes what is left and releases the resources
// it must not be used after Close is called
func (p *Pipeline) Close() {
	p.closed = true
	close(p.pipes[0].in)
}

func bindChannels(from <-chan interface{}, to chan<- interface{}) {
	go func(from <-chan interface{}, to chan<- interface{}) {
		for msg := range from {
			to <- msg
		}

		close(to)
	}(from, to)
}
