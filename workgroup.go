// Copyright 2021 The anthhub. All rights reserved.
// Refer to errgroup

// Package workgroup provides synchronization, result and error propagation, max goroutines
// limit, panic recover and Context customized cancelation for groups of goroutines working
// on subtasks of a common task.
package workgroup

import (
	"context"
	"fmt"
	"runtime"
	"sync"
)

// A group is a collection of goroutines working on subtasks that are part of
// the same overall task.
//
// Group struct is private, A zero group is invalid, getting group must use
// CreatGroup or WithContext.
type group struct {
	fn     chan func() (interface{}, error)
	ret    chan *payload
	done   chan struct{}
	cancel func()

	workerOnce sync.Once
	cancelOnce sync.Once
}

// Group result payload.
type payload struct {
	Data interface{}
	Err  error
}

// Returns a new group
func Create() *group {
	ret := make(chan *payload)
	done := make(chan struct{})
	g := &group{ret: ret, done: done}

	return g
}

// Returns a new group and an associated Context derived from ctx.
func WithContext(ctx context.Context) (*group, context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	g := Create()
	g.cancel = cancel
	return g, ctx
}

// Terminate the channel and context of the group.
func (g *group) Cancel() {
	g.cancelOnce.Do(func() {
		close(g.done)

		if g.cancel != nil {
			g.cancel()
		}

	})
}

// Returns the group result channel. You can for-range it, and then get data and err payload
// like following:
// 	for p range g.Result{
// 		if p.Err != nil {
// 			g.Cancel()
// 			return
// 		}
// 		...
//	}
//
// You can cancel the all subtasks of the group or ignore it when the error occur.
func (g *group) Result() <-chan *payload {
	return g.ret
}

// Go calls the given function in a goroutine, the goroutines number can be limited.
//
// The function calling need return a payload -- data and error.
func (g *group) Go(f func() (interface{}, error)) {
	if g.fn != nil {
		select {
		case <-g.done:
			return

		case g.fn <- f:
		}
		return
	}

	go g.do(f)
}

// Executes the function in a goroutine with recover.
func (g *group) do(f func() (interface{}, error)) {
	var (
		data interface{}
		err  error
	)

	defer func() {
		// avoid goroutines leak.
		select {
		case <-g.done:
			return
		case g.ret <- &payload{data, err}:
		}
	}()

	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 64<<10)
			buf = buf[:runtime.Stack(buf, false)]
			err = fmt.Errorf("workgroup: panic recovered: %s\n%s", r, buf)
		}
	}()

	data, err = f()
}

// It is to set max goroutine to work.
func (g *group) Limit(n int) *group {
	if n <= 0 {
		panic("workgroup: limit must great than 0")
	}
	g.workerOnce.Do(func() {
		g.fn = make(chan func() (interface{}, error), n)

		for i := 0; i < n; i++ {
			go func() {
				for {
					select {
					case <-g.done:
						return
					case f := <-g.fn:
						g.do(f)
					}
				}
			}()
		}
	})

	return g
}
