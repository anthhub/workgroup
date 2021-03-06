# Workgroup

> A simple and useful goroutine concurrent library.


## Installation

```bash
go get github.com/anthhub/workgroup
```

## Usage

```go
	count := 100
	// create a workgroup
	g := Create()

	for i := 0; i < count; i++ {
		g.Go(func() (interface{}, error) {
			// your work function
			return consumer()
		})
	}

	for i := 0; i < count; i++ {
		// get each result
		p := <-g.Result()
		if p.Err != nil {
			return
		}
	}
```

## Advanced Usage

```go
	count := 100
	// create a workgroup with context
	g, ctx := WithContext(context.Background())
	// limit the count of goroutine workers
	g.Limit(5)
	// terminate all subtasks
	defer g.Cancel()

	// the loop need be wrapped by a goroutine when the limit less than your tasks count,
	// else dead lock will be created.
	go func() {
		for i := 0; i < count; i++ {
			g.Go(func() (interface{}, error) {
				// your work function
				// panic will be recover and return a error
				return consumer(ctx)
			})
		}
	}()

	for i := 0; i < count; i++ {
		p := <-g.Result()
		if p.Err != nil {
			return
		}
	}
```