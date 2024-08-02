// Package ssproc is simple multi-transaction process executor with retry and compensation (rollback). Once Job is
// registered, it guarantees that the Job will be executed at least once (see Behaviors section for more details).
// ssproc uses Postgres as its main data storage (for storing Job data and metadata), you can create your own storage
// by implementing Storage interface.
//
// todo oss: complete docs.
package ssproc
