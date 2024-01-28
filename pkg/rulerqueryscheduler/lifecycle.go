package rulerqueryscheduler

import (
	"github.com/cortexproject/cortex/pkg/ring"
)

func (r *RulerScheduler) OnRingInstanceRegister(_ *ring.BasicLifecycler, ringDesc ring.Desc, instanceExists bool, instanceID string, instanceDesc ring.InstanceDesc) (ring.InstanceState, ring.Tokens) {
	// When we initialize the ruler instance in the ring we want to start from
	// a clean situation, so whatever is the state we set it ACTIVE, while we keep existing
	// tokens (if any).
	var tokens []uint32
	if instanceExists {
		tokens = instanceDesc.GetTokens()
	}

	takenTokens := ringDesc.GetTokens()
	newTokens := ring.GenerateTokens(r.cfg.Ring.NumTokens-len(tokens), takenTokens)

	// Tokens sorting will be enforced by the parent caller.
	tokens = append(tokens, newTokens...)

	return ring.ACTIVE, tokens
}

func (r *RulerScheduler) OnRingInstanceTokens(_ *ring.BasicLifecycler, _ ring.Tokens) {}
func (r *RulerScheduler) OnRingInstanceStopping(_ *ring.BasicLifecycler)              {}
func (r *RulerScheduler) OnRingInstanceHeartbeat(_ *ring.BasicLifecycler, _ *ring.Desc, _ *ring.InstanceDesc) {
}
