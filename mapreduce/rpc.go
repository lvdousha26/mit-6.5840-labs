package mapreduce

// The wire types are deliberately tiny. A worker never tells the coordinator
// what it computed, only which task version it finished and which files it
// produced. That is what lets the coordinator discard a late report without
// having to inspect the data.
type GetTaskArgs struct{}

type GetTaskReply struct {
	Task Task
}

type ReportArgs struct {
	Type    TaskType
	ID      int
	Version int
	Files   []string
}

type ReportReply struct{}

type coordinatorRPC struct {
	c *Coordinator
}

func (h *coordinatorRPC) GetTask(_ *GetTaskArgs, reply *GetTaskReply) error {
	reply.Task = h.c.nextTask()
	return nil
}

func (h *coordinatorRPC) ReportTask(args *ReportArgs, _ *ReportReply) error {
	h.c.report(args.Type, args.ID, args.Version, args.Files)
	return nil
}
