package scheduler

import "time"

type RuleSchedulerRequest struct {
	UserID        string
	Namespace     string
	Rulegroup     string
	EvalTimestamp time.Time
}
