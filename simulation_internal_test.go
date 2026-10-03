package curo

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/raj1kshtz/curo/internal/breaker"
	"github.com/raj1kshtz/curo/internal/retry"
	"github.com/raj1kshtz/curo/internal/timeout"
)

var updateGolden = flag.Bool("update", false, "rewrite testdata/simulation.golden")

const (
	simHost   = "api.example"
	simGolden = "testdata/simulation.golden"

	// reportTargets and reportChanges are the documented Report bounds.
	reportTargets = 128
	reportChanges = 256

	// simStepLatency starts the latency step above the 7.5s timeout that
	// normal latency selects, so the step's first answers are cut.
	simStepLatency = 8 * time.Second

	simHeader = `# Curo simulation scorecards. Every run of a scenario takes the same steps,
# so this file only changes when Curo's behavior changes. Review the diff, then
# regenerate it with: go test -run '^TestSimulation$' -update .
#
# Times are virtual and rounded to the millisecond. Callers give up after 1m0s.
# A false rejection or cut hit a request that would have succeeded without
# Curo. Load is attempts per request; fault load counts requests that arrived
# during a fault. Baseline values describe the same requests without Curo.
# Decisions are sampled every second and once after the last request, so a
# decision that changes back within a second is not listed.
`
)

type simWindow struct {
	start time.Duration
	end   time.Duration
}

type simScenario struct {
	scripts  map[string]simScript
	check    func(*testing.T, *simulator)
	name     string
	about    string
	fallback simScript
	watch    []string
	flows    []simFlow
	faults   []simWindow
	until    time.Duration
}

// quiet reports whether a request that arrived at at came before every fault.
func (scenario *simScenario) quiet(at time.Duration) bool {
	return len(scenario.faults) == 0 || at < scenario.faults[0].start
}

// inFault reports whether at falls in a fault window.
func (scenario *simScenario) inFault(at time.Duration) bool {
	for _, fault := range scenario.faults {
		if at >= fault.start && at < fault.end {
			return true
		}
	}
	return false
}

func (scenario *simScenario) run(t *testing.T, mode Mode) *simulator {
	t.Helper()

	sim := newSimulator(t, mode, scenario.scripts, scenario.fallback, scenario.watch)
	sim.run(slices.Clone(scenario.flows), scenario.until)
	return sim
}

func simTraffic(
	group string,
	method string,
	interval time.Duration,
	stop time.Duration,
	hosts ...string,
) simFlow {
	return simFlow{
		group:    group,
		method:   method,
		hosts:    hosts,
		interval: interval,
		stop:     stop,
	}
}

func simHosts(format string, count int) []string {
	hosts := make([]string, count)
	for index := range hosts {
		hosts[index] = fmt.Sprintf(format, index+1)
	}
	return hosts
}

// simNormal answers in 20 to 80ms. One answer in 50 takes 100 to 500ms and
// one in 1,000 takes 1 to 1.5s.
func simNormal() simBehavior {
	return simSometimes(10,
		simHealthy(time.Second, 500*time.Millisecond),
		simSometimes(200,
			simHealthy(100*time.Millisecond, 400*time.Millisecond),
			simHealthy(20*time.Millisecond, 60*time.Millisecond),
		),
	)
}

// simOutage answers normally, then with fault from start until end.
func simOutage(fault simBehavior, start, end time.Duration) simScript {
	return simScript{
		{behavior: simNormal()},
		{from: start, behavior: fault},
		{from: end, behavior: simNormal()},
	}
}

func simScenarios() []simScenario {
	const (
		read  = "read"
		write = "write"
	)
	unavailable := simAnswers(http.StatusServiceUnavailable, 5*time.Millisecond, 5*time.Millisecond)
	failing := simHosts("svc-%02d.example", 10)
	healthy := simHosts("svc-%02d.example", 20)[10:]
	scripts := make(map[string]simScript, len(failing))
	for _, host := range failing {
		scripts[host] = simOutage(unavailable, 10*time.Minute, 13*time.Minute)
	}

	return []simScenario{
		{
			name:    "steady",
			about:   "healthy dependency with a latency tail up to 1.5s",
			scripts: map[string]simScript{simHost: {{behavior: simNormal()}}},
			watch:   []string{simHost},
			flows: []simFlow{
				simTraffic(read, http.MethodGet, 200*time.Millisecond, 30*time.Minute, simHost),
			},
			until: 30 * time.Minute,
			check: checkSteady,
		},
		{
			name:  "latency-step",
			about: "answers take 8 to 9s for 5 minutes",
			scripts: map[string]simScript{simHost: simOutage(
				simHealthy(simStepLatency, time.Second),
				10*time.Minute,
				15*time.Minute,
			)},
			watch: []string{simHost},
			flows: []simFlow{
				simTraffic(read, http.MethodGet, 200*time.Millisecond, 25*time.Minute, simHost),
			},
			faults: []simWindow{{start: 10 * time.Minute, end: 15 * time.Minute}},
			until:  25 * time.Minute,
			check:  checkLatencyStep,
		},
		{
			name:  "intermittent",
			about: "5% of attempts fail with a connection reset for 10 minutes",
			scripts: map[string]simScript{simHost: simOutage(
				simSometimes(500, simResets(5*time.Millisecond), simNormal()),
				5*time.Minute,
				15*time.Minute,
			)},
			watch: []string{simHost},
			flows: []simFlow{
				simTraffic(read, http.MethodGet, 200*time.Millisecond, 20*time.Minute, simHost),
				simTraffic(write, http.MethodPost, time.Second, 20*time.Minute, simHost),
			},
			faults: []simWindow{{start: 5 * time.Minute, end: 15 * time.Minute}},
			until:  20 * time.Minute,
			check:  checkIntermittent,
		},
		{
			name:  "outage",
			about: "every attempt gets 503 for 3 minutes",
			scripts: map[string]simScript{
				simHost: simOutage(unavailable, 10*time.Minute, 13*time.Minute),
			},
			watch: []string{simHost},
			flows: []simFlow{
				simTraffic(read, http.MethodGet, 200*time.Millisecond, 20*time.Minute, simHost),
				simTraffic(write, http.MethodPost, time.Second, 20*time.Minute, simHost),
			},
			faults: []simWindow{{start: 10 * time.Minute, end: 13 * time.Minute}},
			until:  20 * time.Minute,
			check:  checkOutage,
		},
		{
			name:  "rate-limit",
			about: "30% of attempts get 429 with Retry-After for 5 minutes",
			scripts: map[string]simScript{simHost: simOutage(
				simSometimes(3_000, simLimits(5*time.Millisecond), simNormal()),
				10*time.Minute,
				15*time.Minute,
			)},
			watch: []string{simHost},
			flows: []simFlow{
				simTraffic(read, http.MethodGet, 200*time.Millisecond, 20*time.Minute, simHost),
			},
			faults: []simWindow{{start: 10 * time.Minute, end: 15 * time.Minute}},
			until:  20 * time.Minute,
			check:  checkRateLimit,
		},
		{
			name:  "probing",
			about: "every attempt gets a connection reset for 5 minutes, and again for 2 minutes from 45s after recovery",
			scripts: map[string]simScript{simHost: {
				{behavior: simNormal()},
				{from: 10 * time.Minute, behavior: simResets(time.Millisecond)},
				{from: 15 * time.Minute, behavior: simNormal()},
				{from: 15*time.Minute + 45*time.Second, behavior: simResets(time.Millisecond)},
				{from: 17*time.Minute + 45*time.Second, behavior: simNormal()},
			}},
			watch: []string{simHost},
			flows: []simFlow{
				simTraffic(read, http.MethodGet, 200*time.Millisecond, 25*time.Minute, simHost),
			},
			faults: []simWindow{
				{start: 10 * time.Minute, end: 15 * time.Minute},
				{start: 15*time.Minute + 45*time.Second, end: 17*time.Minute + 45*time.Second},
			},
			until: 25 * time.Minute,
			check: checkProbing,
		},
		{
			name:  "hang",
			about: "the dependency stops answering for 5 minutes",
			scripts: map[string]simScript{
				simHost: simOutage(simHangs(), 10*time.Minute, 15*time.Minute),
			},
			watch: []string{simHost},
			flows: []simFlow{
				simTraffic(read, http.MethodGet, 200*time.Millisecond, 25*time.Minute, simHost),
			},
			faults: []simWindow{{start: 10 * time.Minute, end: 15 * time.Minute}},
			until:  25 * time.Minute,
			check:  checkHang,
		},
		{
			name:  "cardinality",
			about: "12,000 distinct hosts beside a dependency that gets 503 for 3 minutes",
			scripts: map[string]simScript{
				simHost: simOutage(unavailable, 10*time.Minute, 13*time.Minute),
			},
			fallback: simScript{{behavior: simNormal()}},
			watch:    []string{simHost},
			flows: []simFlow{
				simTraffic(read, http.MethodGet, 200*time.Millisecond, 20*time.Minute, simHost),
				simTraffic(
					"scan",
					http.MethodGet,
					100*time.Millisecond,
					20*time.Minute,
					simHosts("scan-%05d.example", 12_000)...,
				),
			},
			faults: []simWindow{{start: 10 * time.Minute, end: 13 * time.Minute}},
			until:  20 * time.Minute,
			check:  checkCardinality,
		},
		{
			name:     "simultaneous",
			about:    "10 of 20 dependencies get 503 for the same 3 minutes",
			scripts:  scripts,
			fallback: simScript{{behavior: simNormal()}},
			watch:    []string{failing[0], healthy[0]},
			flows: []simFlow{
				simTraffic("failing", http.MethodGet, 50*time.Millisecond, 20*time.Minute, failing...),
				simTraffic("healthy", http.MethodGet, 50*time.Millisecond, 20*time.Minute, healthy...),
			},
			faults: []simWindow{{start: 10 * time.Minute, end: 13 * time.Minute}},
			until:  20 * time.Minute,
			check:  checkSimultaneous,
		},
	}
}

// TestSimulation runs every scenario in Enforce and in Observe, checks the
// invariants Curo promises, and compares the scorecards with the golden file.
func TestSimulation(t *testing.T) {
	t.Parallel()

	scenarios := simScenarios()
	sections := make([]string, len(scenarios))
	t.Run("scenarios", func(t *testing.T) {
		for index := range scenarios {
			scenario := &scenarios[index]
			t.Run(scenario.name, func(t *testing.T) {
				t.Parallel()

				enforced := scenario.run(t, Enforce)
				checkInvariants(t, scenario, enforced)
				scenario.check(t, enforced)

				observed := scenario.run(t, Observe)
				checkSameTraffic(t, enforced, observed)
				checkTransparent(t, observed)

				sections[index] = renderScenario(scenario, enforced, observed)
			})
		}
	})
	if t.Failed() {
		return
	}
	for index, section := range sections {
		if section == "" {
			t.Logf("scenario %s did not run, so the golden file is left alone",
				scenarios[index].name)
			return
		}
	}

	got := simHeader + strings.Join(sections, "")
	if *updateGolden {
		if err := os.MkdirAll(filepath.Dir(simGolden), 0o750); err != nil {
			t.Fatalf("MkdirAll() error = %v", err)
		}
		if err := os.WriteFile(simGolden, []byte(got), 0o600); err != nil {
			t.Fatalf("WriteFile() error = %v", err)
		}
		return
	}

	data, err := os.ReadFile(simGolden)
	if err != nil {
		t.Fatalf("ReadFile() error = %v; run go test -run '^TestSimulation$' -update .", err)
	}
	want := strings.ReplaceAll(string(data), "\r\n", "\n")
	if line, wantLine, gotLine, ok := simFirstDifference(want, got); ok {
		t.Fatalf("scorecard differs from %s at line %d:\nwant: %s\ngot:  %s\n"+
			"Review the change, then run go test -run '^TestSimulation$' -update .",
			simGolden, line, wantLine, gotLine)
	}
}

// TestSimulationDeterministic runs the scenarios with the most interleaving
// twice and requires identical scorecards.
func TestSimulationDeterministic(t *testing.T) {
	t.Parallel()

	for _, scenario := range simScenarios() {
		if scenario.name != "probing" && scenario.name != "hang" {
			continue
		}
		first := renderScenario(&scenario, scenario.run(t, Enforce), scenario.run(t, Observe))
		second := renderScenario(&scenario, scenario.run(t, Enforce), scenario.run(t, Observe))
		if line, wantLine, gotLine, ok := simFirstDifference(first, second); ok {
			t.Errorf("%s: second run differs at line %d:\nfirst:  %s\nsecond: %s",
				scenario.name, line, wantLine, gotLine)
		}
	}
}

// simFirstDifference returns the first line, counted from one, at which want
// and got differ.
func simFirstDifference(want, got string) (line int, wantLine, gotLine string, differs bool) {
	wantLines := strings.Split(want, "\n")
	gotLines := strings.Split(got, "\n")
	for index := range max(len(wantLines), len(gotLines)) {
		wantLine, gotLine = "", ""
		if index < len(wantLines) {
			wantLine = wantLines[index]
		}
		if index < len(gotLines) {
			gotLine = gotLines[index]
		}
		if wantLine != gotLine || index >= len(wantLines) || index >= len(gotLines) {
			return index + 1, wantLine, gotLine, true
		}
	}
	return 0, "", "", false
}

// simAudit counts violations of each rule and keeps the first message, so a
// broken invariant reports once instead of once per request.
type simAudit struct {
	t      *testing.T
	counts map[string]int
	first  map[string]string
	rules  []string
}

func newSimAudit(t *testing.T) *simAudit {
	return &simAudit{t: t, counts: make(map[string]int), first: make(map[string]string)}
}

func (audit *simAudit) require(ok bool, rule, format string, args ...any) {
	if ok {
		return
	}
	if audit.counts[rule] == 0 {
		audit.rules = append(audit.rules, rule)
		audit.first[rule] = fmt.Sprintf(format, args...)
	}
	audit.counts[rule]++
}

func (audit *simAudit) report() {
	audit.t.Helper()
	for _, rule := range audit.rules {
		audit.t.Errorf("%s: %d violation(s), first: %s", rule, audit.counts[rule], audit.first[rule])
	}
}

func (request *simRequest) String() string {
	return fmt.Sprintf("%s %s at %v", request.method, request.host, request.arrival)
}

// checkInvariants checks the rules that hold for every scenario in Enforce.
func checkInvariants(t *testing.T, scenario *simScenario, sim *simulator) {
	t.Helper()

	audit := newSimAudit(t)
	floor := timeout.DefaultBounds.Minimum
	type target struct{ host, method string }
	targets := make(map[target]bool)
	var attempted, retried, retriedOK, rejected, cut, probes uint64
	for _, request := range sim.requests {
		targets[target{request.host, request.method}] = true
		waited := request.finished - request.arrival
		audit.require(request.attempts <= 2, "at most one retry",
			"%v made %d attempts", request, request.attempts)
		audit.require(waited <= simCallerTimeout, "caller timeout bounds every wait",
			"%v waited %v", request, waited)
		if request.attempts > 0 {
			attempted++
		}
		if request.attempts == 2 {
			retried++
			if request.succeeded() {
				retriedOK++
			}
			audit.require(request.method == http.MethodGet, "only replay-safe requests retry",
				"%v was retried", request)
		}
		if request.rejected() {
			rejected++
			audit.require(request.attempts == 0, "a rejection makes no attempt",
				"%v made %d attempts", request, request.attempts)
		}
		if request.cut() {
			cut++
			audit.require(request.method == http.MethodGet && !request.probe,
				"only reads that are not probes are cut", "%v was cut", request)
			audit.require(waited >= floor, "no cut before the timeout floor",
				"%v was cut after %v", request, waited)
		}
		if request.probe {
			probes++
			audit.require(request.attempts == 1, "a probe is one attempt",
				"%v probe made %d attempts", request, request.attempts)
		}
		if scenario.quiet(request.arrival) {
			audit.require(request.attempts == 1 && !request.rejected() && !request.cut(),
				"no intervention before the first fault", "%v ended with %s after %d attempts",
				request, simOutcome(request), request.attempts)
		}
	}
	audit.report()

	stats := sim.transport.Stats()
	report := sim.transport.Report()
	if stats.InternalFailures != 0 || stats.SelfDisabled {
		t.Errorf("InternalFailures = %d, SelfDisabled = %t, want 0 and false",
			stats.InternalFailures, stats.SelfDisabled)
	}
	if retried*retry.DepositsPerToken > attempted {
		t.Errorf("%d retries for %d attempted requests exceed the budget of one retry per %d",
			retried, attempted, retry.DepositsPerToken)
	}
	if stats.TrackedTargets > reportTargets || len(report.Targets) > reportTargets ||
		len(report.Changes) > reportChanges {
		t.Errorf("tracked %d targets, %d decisions, %d changes, want at most %d, %d, %d",
			stats.TrackedTargets, len(report.Targets), len(report.Changes),
			reportTargets, reportTargets, reportChanges)
	}
	if stats.RetryAttempts != retried || stats.RetrySuccesses != retriedOK ||
		stats.BreakerRejections != rejected || stats.Timeouts != cut ||
		stats.BreakerProbes != probes || stats.ObservedRequests != attempted {
		t.Errorf("Stats retries, retry successes, rejections, timeouts, probes, observed = "+
			"%d, %d, %d, %d, %d, %d; requests show %d, %d, %d, %d, %d, %d",
			stats.RetryAttempts, stats.RetrySuccesses, stats.BreakerRejections,
			stats.Timeouts, stats.BreakerProbes, stats.ObservedRequests,
			retried, retriedOK, rejected, cut, probes, attempted)
	}
	if len(targets) <= reportTargets && stats.OverflowRequests != 0 {
		t.Errorf("OverflowRequests = %d with %d targets, want 0 within capacity",
			stats.OverflowRequests, len(targets))
	}
	if stats.RetryAttempts+stats.RetryBudgetDenials > stats.ObservedRequests {
		t.Errorf("RetryAttempts %d plus RetryBudgetDenials %d exceed ObservedRequests %d",
			stats.RetryAttempts, stats.RetryBudgetDenials, stats.ObservedRequests)
	}
	checkCooldowns(t, sim)
}

// checkSameTraffic requires the Enforce and Observe runs of a scenario to
// send the same requests with the same draws, so their scorecards describe
// one workload.
func checkSameTraffic(t *testing.T, enforced, observed *simulator) {
	t.Helper()

	if len(enforced.requests) != len(observed.requests) {
		t.Fatalf("Enforce sent %d requests, Observe %d",
			len(enforced.requests), len(observed.requests))
	}
	for index, request := range enforced.requests {
		other := observed.requests[index]
		if request.host != other.host || request.method != other.method ||
			request.arrival != other.arrival || request.draws != other.draws {
			t.Fatalf("request %d is %v with draws %x in Enforce, %v with draws %x in Observe",
				index, request, request.draws, other, other.draws)
		}
	}
}

// checkCooldowns requires every probe and rejection to come while its
// target's breaker is open, every probe to wait at least breaker.BaseCooldown
// after the breaker opened or reopened, and an open breaker to admit a probe
// instead of rejecting a request once breaker.MaxCooldown passed.
func checkCooldowns(t *testing.T, sim *simulator) {
	t.Helper()

	type target struct{ host, method string }
	pending := make(map[target][]time.Duration)
	for _, open := range sim.opens {
		key := target{open.request.host, open.request.method}
		pending[key] = append(pending[key], open.at)
	}

	audit := newSimAudit(t)
	since := make(map[target]time.Duration)
	for _, request := range sim.requests {
		key := target{request.host, request.method}
		for len(pending[key]) > 0 && pending[key][0] <= request.arrival {
			since[key] = pending[key][0]
			pending[key] = pending[key][1:]
		}
		start, open := since[key]
		switch {
		case !open:
			audit.require(!request.probe && !request.rejected(), "breaker activity needs an open",
				"%v ended with %s while its breaker was closed", request, simOutcome(request))
		case request.probe:
			audit.require(request.arrival-start >= breaker.BaseCooldown,
				"cooldown at least BaseCooldown", "%v probed %v after its breaker opened",
				request, request.arrival-start)
			switch {
			case request.finished-request.arrival >= breaker.ProbeTimeout:
				since[key] = request.arrival + breaker.ProbeTimeout
			case simProbeHealthy(request):
				delete(since, key)
			default:
				since[key] = request.finished
			}
		case request.rejected():
			audit.require(request.arrival-start < breaker.MaxCooldown,
				"cooldown at most MaxCooldown", "%v rejected %v after its breaker opened",
				request, request.arrival-start)
		}
	}
	audit.report()
}

// simProbeHealthy reports whether a probe's result closes its breaker: the
// dependency answered with neither 429 nor a 5xx status.
func simProbeHealthy(request *simRequest) bool {
	return request.err == nil &&
		request.status != http.StatusTooManyRequests &&
		request.status < http.StatusInternalServerError
}

// checkTransparent requires Observe to leave every request as it would be
// without Curo.
func checkTransparent(t *testing.T, sim *simulator) {
	t.Helper()

	audit := newSimAudit(t)
	for _, request := range sim.requests {
		want := request.baseline
		waited := request.finished - request.arrival
		var same bool
		switch {
		case want.wait() == simCallerTimeout:
			same = errors.Is(request.err, context.Canceled)
		case want.err != nil:
			same = errors.Is(request.err, want.err)
		default:
			same = request.err == nil && request.status == want.status
		}
		audit.require(request.attempts == 1 && waited == want.wait() && same,
			"Observe leaves requests unchanged", "%v ended with %s after %v and %d attempts",
			request, simOutcome(request), waited, request.attempts)
	}
	audit.report()

	stats := sim.transport.Stats()
	if stats.RetryAttempts != 0 || stats.BreakerOpens != 0 || stats.BreakerProbes != 0 ||
		stats.BreakerRejections != 0 || stats.Timeouts != 0 || stats.InternalFailures != 0 {
		t.Errorf("Observe Stats = %+v, want no retry, breaker, timeout, or internal failure", stats)
	}
}

func checkSteady(t *testing.T, sim *simulator) {
	t.Helper()

	if len(sim.timeline) == 0 {
		t.Fatal("no decision was sampled")
	}
	if final := sim.timeline[len(sim.timeline)-1]; final.diagnosis != DiagnosisHealthy {
		t.Errorf("final diagnosis = %v, want Healthy", final.diagnosis)
	}
}

func checkLatencyStep(t *testing.T, sim *simulator) {
	t.Helper()

	firstCut := time.Duration(-1)
	for _, request := range sim.requests {
		if request.cut() && (firstCut < 0 || request.finished < firstCut) {
			firstCut = request.finished
		}
	}
	if firstCut < 0 {
		t.Fatal("no request was cut, so the step never reached the timeout")
	}
	for _, request := range sim.requests {
		if request.cut() && request.arrival > firstCut {
			t.Errorf("%v was cut after the first cut at %v raised the timeout", request, firstCut)
			break
		}
	}
	if stats := sim.transport.Stats(); stats.BreakerOpens != 0 || stats.RetryAttempts != 0 {
		t.Errorf("BreakerOpens = %d, RetryAttempts = %d, want 0: slow answers are not failures",
			stats.BreakerOpens, stats.RetryAttempts)
	}
	if highest := simHighestTimeout(sim); highest <= simStepLatency+time.Second {
		t.Errorf("highest timeout = %v, want above the slowest answer in the step", highest)
	}
}

func checkIntermittent(t *testing.T, sim *simulator) {
	t.Helper()

	reads := simTally(sim, "read", nil)
	if stats := sim.transport.Stats(); stats.BreakerOpens != 0 || stats.Timeouts != 0 {
		t.Errorf("BreakerOpens = %d, Timeouts = %d, want 0", stats.BreakerOpens, stats.Timeouts)
	}
	if reads.ok <= reads.baselineOK {
		t.Errorf("reads ok = %d, want more than %d without Curo", reads.ok, reads.baselineOK)
	}
}

func checkOutage(t *testing.T, sim *simulator) {
	t.Helper()

	opened := make(map[string]int)
	for _, open := range sim.opens {
		opened[open.request.method]++
		if delay := open.at - 10*time.Minute; delay > time.Minute+10*time.Second {
			t.Errorf("%s breaker opened %v into the outage, want at most 1m10s",
				open.request.method, delay)
		}
	}
	if opened[http.MethodGet] != 1 || opened[http.MethodPost] != 1 {
		t.Errorf("breaker opens by method = %v, want one read and one write", opened)
	}
	checkRecovery(t, sim, 13*time.Minute)
}

func checkRateLimit(t *testing.T, sim *simulator) {
	t.Helper()

	if stats := sim.transport.Stats(); stats.RetryAttempts != 0 || stats.BreakerOpens != 0 ||
		stats.Timeouts != 0 {
		t.Errorf("RetryAttempts = %d, BreakerOpens = %d, Timeouts = %d, want 0",
			stats.RetryAttempts, stats.BreakerOpens, stats.Timeouts)
	}
	if !slices.ContainsFunc(sim.timeline, func(entry simEntry) bool {
		return entry.diagnosis == DiagnosisSaturation
	}) {
		t.Error("diagnosis never reached Saturation")
	}
}

func checkProbing(t *testing.T, sim *simulator) {
	t.Helper()

	if len(sim.opens) != 2 {
		t.Fatalf("breaker opened %d times, want 2", len(sim.opens))
	}
	relapse := sim.opens[1].at
	for _, request := range sim.requests {
		if request.probe && request.arrival > relapse {
			if gap := request.arrival - relapse; gap < breaker.MaxCooldown {
				t.Errorf("first probe after the relapse came %v after it, want at least %v: "+
					"reopening within Probation keeps the escalated cooldown", gap, breaker.MaxCooldown)
			}
			break
		}
	}
	checkRecovery(t, sim, 17*time.Minute+45*time.Second)
}

func checkHang(t *testing.T, sim *simulator) {
	t.Helper()

	if highest := simHighestTimeout(sim); highest != timeout.DefaultBounds.Maximum {
		t.Errorf("highest timeout = %v, want the %v ceiling", highest, timeout.DefaultBounds.Maximum)
	}
	if len(sim.opens) == 0 {
		t.Error("breaker never opened")
	}
	if baseline := simBaselinePeak(sim.requests); sim.peak >= baseline {
		t.Errorf("peak in flight = %d, want below %d without Curo", sim.peak, baseline)
	}
	reads := simTally(sim, "read", nil)
	if reads.falseCut != 0 {
		t.Errorf("false cuts = %d, want 0", reads.falseCut)
	}
	if probes := sim.transport.Stats().BreakerProbes; probes < 2 {
		t.Errorf("BreakerProbes = %d, want an expired probe lease to allow another probe", probes)
	}
}

func checkCardinality(t *testing.T, sim *simulator) {
	t.Helper()

	stats := sim.transport.Stats()
	if stats.TrackedTargets != reportTargets || stats.OverflowRequests == 0 {
		t.Errorf("TrackedTargets = %d, OverflowRequests = %d, want %d and some overflow",
			stats.TrackedTargets, stats.OverflowRequests, reportTargets)
	}
	scan := simTally(sim, "scan", nil)
	if scan.rejected != 0 || scan.cut != 0 || scan.retried != 0 {
		t.Errorf("scan rejected, cut, retried = %d, %d, %d, want 0",
			scan.rejected, scan.cut, scan.retried)
	}
	if len(sim.opens) != 1 || sim.opens[0].request.host != simHost {
		t.Errorf("breaker opens = %d, want exactly one for %s", len(sim.opens), simHost)
	}
	late := false
	for _, decision := range sim.transport.Report().Targets {
		late = late || decision.Target.Host > "scan-09000.example"
	}
	if !late {
		t.Error("no scan host admitted after 15 minutes replaced an expired target")
	}
}

func checkSimultaneous(t *testing.T, sim *simulator) {
	t.Helper()

	opened := make(map[string]int)
	for _, open := range sim.opens {
		opened[open.request.host]++
	}
	for _, host := range simHosts("svc-%02d.example", 10) {
		if opened[host] != 1 {
			t.Errorf("%s breaker opened %d times, want 1", host, opened[host])
		}
	}
	if len(opened) != 10 {
		t.Errorf("breakers opened for %d hosts, want 10", len(opened))
	}
	healthy := simTally(sim, "healthy", nil)
	if healthy.rejected != 0 || healthy.cut != 0 || healthy.retried != 0 {
		t.Errorf("healthy rejected, cut, retried = %d, %d, %d, want 0",
			healthy.rejected, healthy.cut, healthy.retried)
	}
}

// checkRecovery requires every request that arrived after recovered to
// succeed once the breaker had time to probe at breaker.MaxCooldown.
func checkRecovery(t *testing.T, sim *simulator, recovered time.Duration) {
	t.Helper()

	settled := recovered + breaker.MaxCooldown + time.Second
	for _, request := range sim.requests {
		if request.arrival >= settled && !request.succeeded() {
			t.Errorf("%v ended with %s after recovery", request, simOutcome(request))
			return
		}
	}
}

func simHighestTimeout(sim *simulator) time.Duration {
	var highest time.Duration
	for _, entry := range sim.timeline {
		highest = max(highest, entry.timeout)
	}
	return highest
}

func simOutcome(request *simRequest) string {
	switch {
	case request.err == nil:
		return fmt.Sprint(request.status)
	case request.rejected():
		return "rejected"
	case request.cut():
		return "cut"
	case errors.Is(request.err, context.Canceled):
		return "abandoned"
	case errors.Is(request.err, errSimReset):
		return "reset"
	default:
		return request.err.Error()
	}
}

type simCounts struct {
	waits         []time.Duration
	baselineWaits []time.Duration
	requests      int
	attempts      int
	faultRequests int
	faultAttempts int
	ok            int
	baselineOK    int
	rejected      int
	falseRejected int
	cut           int
	falseCut      int
	retried       int
}

// simTally counts the requests of group, or of every group when group is
// empty. A non-nil scenario marks requests that arrived during its faults.
func simTally(sim *simulator, group string, scenario *simScenario) simCounts {
	var counts simCounts
	for _, request := range sim.requests {
		if group != "" && request.group != group {
			continue
		}
		counts.requests++
		counts.attempts += request.attempts
		if scenario != nil && scenario.inFault(request.arrival) {
			counts.faultRequests++
			counts.faultAttempts += request.attempts
		}
		counts.waits = append(counts.waits, request.finished-request.arrival)
		counts.baselineWaits = append(counts.baselineWaits, request.baseline.wait())
		wanted := request.baseline.succeeded()
		if wanted {
			counts.baselineOK++
		}
		if request.succeeded() {
			counts.ok++
		}
		if request.attempts == 2 {
			counts.retried++
		}
		if request.rejected() {
			counts.rejected++
			if wanted {
				counts.falseRejected++
			}
		}
		if request.cut() {
			counts.cut++
			if wanted {
				counts.falseCut++
			}
		}
	}
	return counts
}

// simBaselinePeak returns the most requests in flight at once without Curo.
func simBaselinePeak(requests []*simRequest) int {
	type change struct {
		at    time.Duration
		delta int
	}
	changes := make([]change, 0, 2*len(requests))
	for _, request := range requests {
		changes = append(changes,
			change{at: request.arrival, delta: 1},
			change{at: request.arrival + request.baseline.wait(), delta: -1},
		)
	}
	// A request that ends when another arrives does not overlap it.
	slices.SortFunc(changes, func(a, b change) int {
		if order := cmp.Compare(a.at, b.at); order != 0 {
			return order
		}
		return cmp.Compare(a.delta, b.delta)
	})
	peak, current := 0, 0
	for _, change := range changes {
		current += change.delta
		peak = max(peak, current)
	}
	return peak
}

func simRatio(numerator, denominator int) string {
	if denominator == 0 {
		return "-"
	}
	scaled := numerator * 1000 / denominator
	return fmt.Sprintf("%d.%03d", scaled/1000, scaled%1000)
}

func simTime(value time.Duration) string {
	return value.Round(time.Millisecond).String()
}

func simWaits(waits []time.Duration) string {
	sorted := slices.Clone(waits)
	slices.Sort(sorted)
	var total time.Duration
	for _, wait := range sorted {
		total += wait
	}
	percentile := func(permille int) time.Duration {
		rank := (len(sorted)*permille + 999) / 1000
		return sorted[max(rank, 1)-1]
	}
	return fmt.Sprintf("mean=%s p50=%s p99=%s max=%s",
		simTime(total/time.Duration(len(sorted))),
		simTime(percentile(500)),
		simTime(percentile(990)),
		simTime(sorted[len(sorted)-1]))
}

func renderScenario(scenario *simScenario, sim, observed *simulator) string {
	var out strings.Builder
	fmt.Fprintf(&out, "\n== %s: %s\n", scenario.name, scenario.about)

	var groups []string
	for _, flow := range scenario.flows {
		if !slices.Contains(groups, flow.group) {
			groups = append(groups, flow.group)
		}
	}
	for _, group := range groups {
		counts := simTally(sim, group, scenario)
		fmt.Fprintf(&out, "%s: requests=%d attempts=%d load=%s fault_load=%s\n",
			group, counts.requests, counts.attempts,
			simRatio(counts.attempts, counts.requests),
			simRatio(counts.faultAttempts, counts.faultRequests))
		fmt.Fprintf(&out, "  ok=%d baseline_ok=%d rejected=%d false_rejected=%d "+
			"cut=%d false_cut=%d retried=%d\n",
			counts.ok, counts.baselineOK, counts.rejected, counts.falseRejected,
			counts.cut, counts.falseCut, counts.retried)
		fmt.Fprintf(&out, "  wait %s\n", simWaits(counts.waits))
		fmt.Fprintf(&out, "  baseline wait %s\n", simWaits(counts.baselineWaits))
	}

	for _, fault := range scenario.faults {
		detection, recovery := "-", "-"
		for _, open := range sim.opens {
			if open.at >= fault.start && open.at < fault.end {
				detection = simTime(open.at - fault.start)
				break
			}
		}
		for _, request := range sim.requests {
			if request.arrival >= fault.end && request.succeeded() {
				recovery = simTime(request.finished - fault.end)
				break
			}
		}
		fmt.Fprintf(&out, "fault %s-%s: detection=%s recovery=%s\n",
			simTime(fault.start), simTime(fault.end), detection, recovery)
	}

	stats := sim.transport.Stats()
	report := sim.transport.Report()
	fmt.Fprintf(&out, "stats: retries=%d retry_ok=%d denied=%d opens=%d probes=%d "+
		"rejections=%d timeouts=%d internal=%d\n",
		stats.RetryAttempts, stats.RetrySuccesses, stats.RetryBudgetDenials,
		stats.BreakerOpens, stats.BreakerProbes, stats.BreakerRejections,
		stats.Timeouts, stats.InternalFailures)
	fmt.Fprintf(&out, "state: tracked=%d overflow=%d decisions=%d changes=%d "+
		"peak_in_flight=%d baseline_peak_in_flight=%d\n",
		stats.TrackedTargets, stats.OverflowRequests, len(report.Targets),
		len(report.Changes), sim.peak, simBaselinePeak(sim.requests))

	type event struct {
		text string
		at   time.Duration
	}
	var events []event
	for _, open := range sim.opens {
		events = append(events, event{
			at:   open.at,
			text: fmt.Sprintf("open %s %s", open.request.host, open.request.method),
		})
	}
	for _, request := range sim.requests {
		if request.probe {
			events = append(events, event{
				at: request.arrival,
				text: fmt.Sprintf("probe %s %s -> %s after %s", request.host, request.method,
					simOutcome(request), simTime(request.finished-request.arrival)),
			})
		}
	}
	slices.SortStableFunc(events, func(a, b event) int {
		return cmp.Compare(a.at, b.at)
	})
	out.WriteString("breaker:")
	if len(events) == 0 {
		out.WriteString(" none")
	}
	out.WriteString("\n")
	for _, event := range events {
		fmt.Fprintf(&out, "  %s %s\n", simTime(event.at), event.text)
	}

	out.WriteString("decisions:\n")
	for _, entry := range sim.timeline {
		fmt.Fprintf(&out, "  %s %s %s %s candidates=%s timeout=%s\n",
			simTime(entry.at), entry.target.Host, entry.target.Method,
			entry.diagnosis, entry.candidates, entry.timeout)
	}

	shadow := observed.transport.Stats().ShadowTimeouts
	fmt.Fprintf(&out, "observe: shadow_timeouts=%d changes=%d\n",
		shadow, len(observed.transport.Report().Changes))

	return out.String()
}
