package boomer

import "fmt"

// RunContext RunContext
type RunContext struct{
	ID     		int			`json:"id"`
	RunSeq 		int			`json:"runSeq"`
	RspHead   string	`json:"rspHead"`
	RspCookie string  `json:"rspCookie"`
	RspStatus int     `json:"rspStatus"`
	RspJSON   string  `json:"rspJSON"`
	RspText   string  `json:"rspText"`
	RunHost   string  `json:"runHost"`
	Store     map[string] string
}
func (ctx RunContext) ToString() string{

	str:="ctx=\n"
	str+="  {\n"
	str+=fmt.Sprintf("    .ID=%d\n",ctx.ID)
	str+=fmt.Sprintf("    .RunSeq=%d\n",ctx.RunSeq)
	str+=fmt.Sprintf("    .RspHead=%s\n",ctx.RspHead)
	str+=fmt.Sprintf("    .RspCookie=%s\n",ctx.RspCookie)
	str+=fmt.Sprintf("    .RspStatus=%d\n",ctx.RspStatus)
	str+=fmt.Sprintf("    .RspJSON=%s\n",ctx.RspJSON)
	str+=fmt.Sprintf("    .RspText=%s\n",ctx.RspText)
	str+=fmt.Sprintf("    .Store=%s\n",ctx.Store)
	str+="  }\n"
	return str
}
//NewRunContext NewRunContext
func NewRunContext() *RunContext {
	ctx:=RunContext{}
	ctx.ID=0
	ctx.RunSeq=0
	ctx.Store=map[string]string{}
	return &ctx
}
// Task is like the "Locust object" in locust, the python version.
// When boomer receives a start message from master, it will spawn several goroutines to run Task.Fn.
// But users can keep some information in the python version, they can't do the same things in boomer.
// Because Task.Fn is a pure function.
type Task struct {
	// The weight is used to distribute goroutines over multiple tasks.
	Weight int
	// Fn is called by the goroutines allocated to this task, in a loop.
	Fn   func(ctx *RunContext)
	Name string
}
