package qtx

type trmResponseCode int

const (
	trmResponseCodeDone trmResponseCode = iota
	trmResponseCodeAbort
	trmResponseCodeCommit
)

// ---

type trmResponse struct {
	code  trmResponseCode
	enlId int
	cause error
}

// ---

type enlistment struct {
	id   int
	resp chan<- trmResponse
}

func (en enlistment) Done() {
	en.resp <- trmResponse{code: trmResponseCodeDone, enlId: en.id}
}

func (en enlistment) ForceRollback(cause error) {
	en.resp <- trmResponse{code: trmResponseCodeAbort, enlId: en.id, cause: cause}
}

func (en enlistment) Prepared() {
	en.resp <- trmResponse{code: trmResponseCodeCommit, enlId: en.id}
}

func (en enlistment) Aborted(cause error) {
	en.resp <- trmResponse{code: trmResponseCodeAbort, enlId: en.id, cause: cause}
}

func (en enlistment) Committed() {
	en.resp <- trmResponse{code: trmResponseCodeCommit, enlId: en.id}
}
