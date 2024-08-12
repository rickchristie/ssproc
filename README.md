# `ssproc`

todo oss: Write documentation.

todo oss: Create 

## Getting Started

Pass Postgres connection string, schema and table:

```go
package main

import "context"

func main() {
	executor, _, err := ssproc.Initialize(
		context.Background(),
		connectionStr,
		schema,
		table,
		process,
		executorConfig,
	)
}
```

