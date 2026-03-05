package internal

// TargetNotification is the message published over PG NOTIFY to coordinate
// target lifecycle changes across processes.
type TargetNotification struct {
	Name   string `json:"name"`
	Action string `json:"action"`
}

const (
	TargetActionConfigure = "configure"
	TargetActionRemove    = "remove"
	TargetActionStart     = "start"
	TargetActionStop      = "stop"

	TargetNotifyChannel = "replicant_target"
)
