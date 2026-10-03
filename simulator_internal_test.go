package curo

import (
	"cmp"
	"container/heap"
	"context"
	"errors"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/raj1kshtz/curo/internal/breaker"
	"github.com/raj1kshtz/curo/internal/retry"
	"github.com/raj1kshtz/curo/internal/timeout"
)

// simCallerTimeout is how long a simulated caller waits for its request
// before it cancels the request's context.
const simCallerTimeout = time.Minute

var errSimReset = errors.New("simulated connection reset")

// simKey is the context key under which a request carries its *simRequest.
type simKey struct{}

// simFate is how a simulated dependency answers one attempt.
type simFate struct {
	err     error
	header  http.Header
	latency time.Duration
	status  int
	hang    bool
}

// wait returns how long a caller waits for this answer.
func (answer simFate) wait() time.Duration {
	if answer.hang || answer.latency >= simCallerTimeout {
		return simCallerTimeout
	}
	return answer.latency
}

// succeeded reports whether the caller gets a successful response before it
// gives up.
func (answer simFate) succeeded() bool {
	return answer.err == nil &&
		answer.wait() < simCallerTimeout &&
		simSuccessful(answer.status)
}

func simSuccessful(status int) bool {
	return status >= http.StatusOK && status < http.StatusBadRequest
}

// simBehavior answers one attempt from a uniformly distributed draw.
type simBehavior func(draw uint64) simFate

func simAnswers(status int, base, spread time.Duration) simBehavior {
	return func(draw uint64) simFate {
		return simFate{
			status:  status,
			latency: base + time.Duration(draw%uint64(spread+1)),
		}
	}
}

func simHealthy(base, spread time.Duration) simBehavior {
	return simAnswers(http.StatusOK, base, spread)
}

func simResets(latency time.Duration) simBehavior {
	return func(uint64) simFate {
		return simFate{err: errSimReset, latency: latency}
	}
}

func simHangs() simBehavior {
	return func(uint64) simFate {
		return simFate{hang: true}
	}
}

func simLimits(latency time.Duration) simBehavior {
	header := http.Header{"Retry-After": {"1"}}
	return func(uint64) simFate {
		return simFate{
			status:  http.StatusTooManyRequests,
			header:  header,
			latency: latency,
		}
	}
}

// simSometimes answers with rare for perTenThousand of 10,000 draws and with
// otherwise for the rest. Both get the remaining bits of the draw.
func simSometimes(perTenThousand uint64, rare, otherwise simBehavior) simBehavior {
	return func(draw uint64) simFate {
		if draw%10_000 < perTenThousand {
			return rare(draw / 10_000)
		}
		return otherwise(draw / 10_000)
	}
}

// simPhase is a behavior that applies from a virtual time until the next
// phase starts.
type simPhase struct {
	behavior simBehavior
	from     time.Duration
}

// simScript is the behavior of one dependency over virtual time. Its first
// phase starts at zero.
type simScript []simPhase

func (script simScript) answer(at time.Duration, draw uint64) simFate {
	current := script[0].behavior
	for _, next := range script[1:] {
		if at < next.from {
			break
		}
		current = next.behavior
	}

	return current(draw)
}

// simFlow is periodic traffic from one group of callers. Its nth request goes
// to hosts[n % len(hosts)].
type simFlow struct {
	group    string
	method   string
	hosts    []string
	start    time.Duration
	stop     time.Duration
	interval time.Duration
	sent     int
}

// simRequest records one simulated request. Draws fix how the dependency
// answers its initial attempt and its retry, and baseline is how the
// dependency would answer the request without Curo.
type simRequest struct {
	err      error
	request  *http.Request
	cancel   context.CancelFunc
	wait     *simWait
	callerAt *simEvent
	method   string
	group    string
	host     string
	baseline simFate
	draws    [2]uint64
	arrival  time.Duration
	finished time.Duration
	status   int
	attempts int
	probe    bool
	done     bool
}

func (request *simRequest) succeeded() bool {
	return request.err == nil && simSuccessful(request.status)
}

func (request *simRequest) rejected() bool {
	return errors.Is(request.err, ErrBreakerOpen)
}

func (request *simRequest) cut() bool {
	return errors.Is(request.err, ErrTimeout)
}

// simWait is a point where a request goroutine waits for the scheduler: an
// attempt in the simulated dependency, or a retry backoff.
type simWait struct {
	ctx       context.Context
	wake      chan struct{}
	event     *simEvent
	completed bool
}

type simEvent struct {
	run      func()
	at       time.Duration
	seq      uint64
	canceled bool
}

// simQueue orders events by virtual time, then by scheduling order.
type simQueue []*simEvent

func (queue simQueue) Len() int { return len(queue) }

func (queue simQueue) Less(i, j int) bool {
	if queue[i].at != queue[j].at {
		return queue[i].at < queue[j].at
	}
	return queue[i].seq < queue[j].seq
}

func (queue simQueue) Swap(i, j int) {
	queue[i], queue[j] = queue[j], queue[i]
}

func (queue *simQueue) Push(value any) {
	event, _ := value.(*simEvent)
	*queue = append(*queue, event)
}

func (queue *simQueue) Pop() any {
	old := *queue
	last := len(old) - 1
	event := old[last]
	old[last] = nil
	*queue = old[:last]
	return event
}

type simTimer struct {
	event *simEvent
	fired bool
}

func (timer *simTimer) Stop() bool {
	active := !timer.fired && !timer.event.canceled
	timer.event.canceled = true
	return active
}

// simOpen is a breaker opening, attributed to the request whose failure
// opened it.
type simOpen struct {
	request *simRequest
	at      time.Duration
}

// simEntry is a sampled decision of a watched target that differs from the
// target's previous entry.
type simEntry struct {
	target     Target
	at         time.Duration
	timeout    time.Duration
	diagnosis  Diagnosis
	candidates Candidates
}

// simulator drives a Transport with scripted traffic in virtual time.
//
// Exactly one goroutine runs at a time: the scheduler, which runs the test,
// or the one request goroutine it handed control to. A request goroutine
// hands control back when it waits in the simulated dependency or in a retry
// backoff, and when its RoundTrip returns. Every run of a scenario therefore
// takes the same steps in the same order.
//
// Requests draw from traffic and retry backoffs draw from backoff, two
// separate random sequences, so every mode sends the same requests with the
// same draws however many retries Curo makes.
type simulator struct {
	origin    time.Time
	t         testing.TB
	latest    map[Target]int
	watch     map[string]bool
	scripts   map[string]simScript
	current   *simRequest
	yield     chan struct{}
	transport *Transport
	timeline  []simEntry
	requests  []*simRequest
	opens     []simOpen
	queue     simQueue
	fallback  simScript
	stats     Stats
	now       time.Duration
	seq       uint64
	traffic   uint64
	backoff   uint64
	inflight  int
	peak      int
}

func newSimulator(
	t testing.TB,
	mode Mode,
	scripts map[string]simScript,
	fallback simScript,
	watch []string,
) *simulator {
	t.Helper()

	sim := &simulator{
		t:        t,
		scripts:  scripts,
		fallback: fallback,
		watch:    make(map[string]bool, len(watch)),
		latest:   make(map[Target]int),
		yield:    make(chan struct{}),
		origin:   time.Unix(1_900_000_000, 0).UTC(),
		traffic:  1,
		backoff:  2,
	}
	for _, host := range watch {
		sim.watch[host] = true
	}

	transport := newClockedTransport(
		t,
		internalRoundTripperFunc(sim.roundTrip),
		sim.clock,
		WithMode(mode),
	)
	stages := newAdaptiveStages(
		transport.observer,
		&transport.retries,
		&transport.breakers,
		&transport.timeouts,
		transport.authorized,
	)
	stages.jitter = sim.jitter
	transport.stages = simStages{requestStages: stages}
	transport.wait = sim.wait
	transport.after = sim.after
	sim.transport = transport

	return sim
}

// simStages marks each request that an open breaker admits as a probe, so
// probes are known without relying on Stats.
type simStages struct {
	requestStages
}

func (stages simStages) preflight(
	snapshot requestSnapshot,
	authority uint64,
) (requestState, error) {
	state, err := stages.requestStages.preflight(snapshot, authority)
	if request, ok := snapshot.requestContext.Value(simKey{}).(*simRequest); ok &&
		state.admission == breaker.Probe {
		request.probe = true
	}
	return state, err
}

func (sim *simulator) clock() time.Time {
	return sim.origin.Add(sim.now)
}

// simNext returns the next value of the splitmix64 sequence with state.
// Unlike math/rand, its output never changes between Go releases.
func simNext(state *uint64) uint64 {
	*state += 0x9e3779b97f4a7c15
	value := *state
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func (sim *simulator) jitter() time.Duration {
	span := uint64(retry.MaxBackoff-retry.MinBackoff) + 1
	return retry.MinBackoff + time.Duration(simNext(&sim.backoff)%span)
}

func (sim *simulator) script(host string) simScript {
	if script, ok := sim.scripts[host]; ok {
		return script
	}
	return sim.fallback
}

func (sim *simulator) schedule(at time.Duration, run func()) *simEvent {
	sim.seq++
	event := &simEvent{at: at, seq: sim.seq, run: run}
	heap.Push(&sim.queue, event)
	return event
}

// run sends flows and samples watched decisions every second until until,
// then waits for every request to finish and samples them once more.
func (sim *simulator) run(flows []simFlow, until time.Duration) {
	sim.t.Helper()

	for index := range flows {
		flow := &flows[index]
		sim.schedule(flow.start, func() { sim.arrive(flow) })
	}
	var sample func()
	sample = func() {
		sim.sample()
		if next := sim.now + time.Second; next <= until {
			sim.schedule(next, sample)
		}
	}
	sim.schedule(time.Second, sample)

	for sim.queue.Len() > 0 {
		event, _ := heap.Pop(&sim.queue).(*simEvent)
		if event.canceled {
			continue
		}
		sim.now = event.at
		event.run()
	}
	sim.sample()

	for _, request := range sim.requests {
		if !request.done {
			sim.t.Fatalf("request to %s at %v never finished", request.host, request.arrival)
		}
	}
}

func (sim *simulator) arrive(flow *simFlow) {
	if next := sim.now + flow.interval; next < flow.stop {
		sim.schedule(next, func() { sim.arrive(flow) })
	}

	host := flow.hosts[flow.sent%len(flow.hosts)]
	flow.sent++
	request := &simRequest{
		group:   flow.group,
		host:    host,
		method:  flow.method,
		arrival: sim.now,
		draws:   [2]uint64{simNext(&sim.traffic), simNext(&sim.traffic)},
	}
	request.baseline = sim.script(host).answer(sim.now, request.draws[0])

	ctx, cancel := context.WithCancel(context.Background())
	outbound, err := http.NewRequestWithContext(
		context.WithValue(ctx, simKey{}, request),
		flow.method,
		"https://"+host+"/items",
		nil,
	)
	if err != nil {
		cancel()
		sim.t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	request.request = outbound
	request.cancel = cancel
	request.callerAt = sim.schedule(sim.now+simCallerTimeout, func() {
		sim.giveUp(request)
	})
	sim.requests = append(sim.requests, request)

	sim.current = request
	go sim.serve(request)
	<-sim.yield
	sim.step(request)
}

func (sim *simulator) serve(request *simRequest) {
	response, err := sim.transport.RoundTrip(request.request)
	if response != nil {
		request.status = response.StatusCode
		_ = response.Body.Close()
	}
	request.err = err
	request.finished = sim.now
	request.done = true
	request.callerAt.canceled = true
	request.cancel()
	sim.yield <- struct{}{}
}

// step runs on the scheduler after request handed control back. It
// attributes each breaker opening to the request that caused it.
func (sim *simulator) step(request *simRequest) {
	stats := sim.transport.Stats()
	if stats.BreakerOpens > sim.stats.BreakerOpens {
		sim.opens = append(sim.opens, simOpen{request: request, at: sim.now})
	}
	sim.stats = stats
}

// sample appends the decisions of watched targets that changed since the
// previous sample, ordered by evaluation time.
func (sim *simulator) sample() {
	start := len(sim.timeline)
	for _, decision := range sim.transport.Report().Targets {
		if !sim.watch[decision.Target.Host] {
			continue
		}
		entry := simEntry{
			target:     decision.Target,
			at:         decision.EvaluatedAt.Sub(sim.origin),
			timeout:    decision.Timeout,
			diagnosis:  decision.Diagnosis,
			candidates: decision.Candidates,
		}
		if index, ok := sim.latest[entry.target]; ok {
			previous := sim.timeline[index]
			if previous.diagnosis == entry.diagnosis &&
				previous.candidates == entry.candidates &&
				previous.timeout == entry.timeout {
				continue
			}
		}
		sim.timeline = append(sim.timeline, entry)
	}

	added := sim.timeline[start:]
	slices.SortStableFunc(added, func(left, right simEntry) int {
		return cmp.Compare(left.at, right.at)
	})
	for offset, entry := range added {
		sim.latest[entry.target] = start + offset
	}
}

// block hands control back to the scheduler until resume wakes request.
func (sim *simulator) block(request *simRequest, wait *simWait) {
	request.wait = wait
	sim.inflight++
	sim.peak = max(sim.peak, sim.inflight)
	sim.yield <- struct{}{}
	<-wait.wake
}

// resume hands control to request if it still waits at wait, and takes it
// back once request blocks again or finishes.
func (sim *simulator) resume(request *simRequest, wait *simWait, completed bool) {
	if request.wait != wait {
		return
	}
	request.wait = nil
	sim.inflight--
	wait.completed = completed
	if wait.event != nil {
		wait.event.canceled = true
	}

	sim.current = request
	wait.wake <- struct{}{}
	<-sim.yield
	sim.step(request)
}

func (sim *simulator) giveUp(request *simRequest) {
	request.cancel()
	if wait := request.wait; wait != nil && wait.ctx.Err() != nil {
		sim.resume(request, wait, false)
	}
}

func (sim *simulator) roundTrip(req *http.Request) (*http.Response, error) {
	request, _ := req.Context().Value(simKey{}).(*simRequest)
	draw := request.draws[min(request.attempts, len(request.draws)-1)]
	request.attempts++
	answer := sim.script(request.host).answer(sim.now, draw)

	wait := &simWait{ctx: req.Context(), wake: make(chan struct{})}
	if !answer.hang {
		wait.event = sim.schedule(sim.now+answer.latency, func() {
			sim.resume(request, wait, true)
		})
	}
	sim.block(request, wait)
	if !wait.completed {
		return nil, req.Context().Err()
	}
	if answer.err != nil {
		return nil, answer.err
	}

	return &http.Response{
		StatusCode: answer.status,
		Header:     answer.header,
		Body:       http.NoBody,
	}, nil
}

func (sim *simulator) wait(ctx context.Context, cancel <-chan struct{}, delay time.Duration) bool {
	if ctx.Err() != nil {
		return false
	}

	request, _ := ctx.Value(simKey{}).(*simRequest)
	wait := &simWait{ctx: ctx, wake: make(chan struct{})}
	wait.event = sim.schedule(sim.now+delay, func() {
		sim.resume(request, wait, true)
	})
	sim.block(request, wait)

	return ctx.Err() == nil && !retry.Canceled(cancel)
}

// after starts an adaptive timeout for the attempt that the running request
// is about to send. When it fires, it wakes the attempt if the timeout ended
// it.
func (sim *simulator) after(limit time.Duration, fire func()) timeout.Timer {
	request := sim.current
	timer := &simTimer{}
	timer.event = sim.schedule(sim.now+limit, func() {
		timer.fired = true
		fire()
		if wait := request.wait; wait != nil && wait.ctx.Err() != nil {
			sim.resume(request, wait, false)
		}
	})

	return timer
}
