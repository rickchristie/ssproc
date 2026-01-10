package ssproc

import "errors"

var AlreadyLeased = errors.New("job is leased by another executor")
var AlreadyDone = errors.New("job status is already done")
var AlreadyCompensated = errors.New("job status is already compensated")
var AlreadyError = errors.New("job status is already in-error")

var MarkedAsError = errors.New("max execution count reached, job status updated to 'error'")
var MarkedAsCompensated = errors.New("compensation completed, job status updated to 'compensated'")
var TransitionedToCompensation = errors.New("max execution count reached, transitioning to compensation")
var ContextCanceled = errors.New("context canceled on execution")
var ContextCanceledOngoing = errors.New("context cancelled while waiting for subprocess execution")
var SubprocessFailed = errors.New("subprocess execution failed")
var CompensationFailed = errors.New("compensation execution failed")

var JobIdAlreadyExist = errors.New("job with same ID already exists")
