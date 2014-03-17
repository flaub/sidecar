package main

type QueueMessage struct {
	index int
	item  interface{}
}

type QueueWorker func(item interface{}) interface{}

type ParallelOrderedQueue struct {
	worker QueueWorker
	input  chan *QueueMessage
	queue  chan *QueueMessage
	output chan *QueueMessage
	cur    *QueueMessage
	last   int
}

func NewParallelOrderedQueue(numWorkers int, worker QueueWorker) *ParallelOrderedQueue {
	this := &ParallelOrderedQueue{
		worker: worker,
		input:  make(chan *QueueMessage),
		queue:  make(chan *QueueMessage),
		output: make(chan *QueueMessage),
	}
	for i := 0; i < numWorkers; i++ {
		go this.Run()
	}
	go this.Sort()
	return this
}

func (this *ParallelOrderedQueue) Add(item interface{}) {
	this.input <- &QueueMessage{this.last, item}
	this.last += 1
}

func (this *ParallelOrderedQueue) End() {
	this.input <- nil
}

func (this *ParallelOrderedQueue) Run() {
	for {
		msg, ok := <-this.input
		if !ok {
			return
		}
		if msg == nil {
			close(this.input)
			this.queue <- nil
			return
		}
		msg.item = this.worker(msg.item)
		this.queue <- msg
	}
}

func (this *ParallelOrderedQueue) Sort() {
	final := false
	expect := 0
	hold := make(map[int]*QueueMessage)
	for {
		if final && expect == this.last {
			close(this.queue)
			close(this.output)
			return
		}
		msg := <-this.queue
		if msg == nil {
			final = true
			continue
		}
		if msg.index == expect {
			this.output <- msg
			// suck up any others that might be done already
			for {
				expect++
				msg, ok := hold[expect]
				if !ok {
					break
				}
				if msg.index == expect {
					delete(hold, expect)
					this.output <- msg
				}
			}
		} else {
			hold[msg.index] = msg
		}
	}
}

// blocks until next available result. returns false if no more results are available.
func (this *ParallelOrderedQueue) Next() bool {
	msg, ok := <-this.output
	if !ok {
		return false
	}
	this.cur = msg
	return true
}

func (this *ParallelOrderedQueue) Current() interface{} {
	return this.cur.item
}
