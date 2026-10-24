package store

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// Redis is a Store backed by Redis.
//
// All keys share a hash tag ({prefix}), so they live in the same slot and the
// Lua scripts below also work with Redis Cluster.
type Redis struct {
	client redis.UniversalClient
	keys   keys
}

var _ Store = (*Redis)(nil)

type keys struct {
	state     string // string: State
	mode      string // string: dedup mode of the current run
	processed string // integer: records received
	unique    string // integer: distinct keys
	skipped   string // integer: distinct keys without a sender
	seen      string // set: dedup keys seen so far
	senders   string // sorted set: sender -> count of distinct emails
	err       string // string: reason passed to Fail
	owner     string // string: token of the current or last run
	lease     string // string with a TTL: exists while the running run is alive
}

// NewRedis returns a Store that keeps its data in client under keys starting with {prefix}.
func NewRedis(client redis.UniversalClient, prefix string) *Redis {
	k := func(name string) string { return "{" + prefix + "}:" + name }
	return &Redis{
		client: client,
		keys: keys{
			state:     k("state"),
			mode:      k("mode"),
			processed: k("processed"),
			unique:    k("unique"),
			skipped:   k("skipped"),
			seen:      k("seen"),
			senders:   k("senders"),
			err:       k("error"),
			owner:     k("owner"),
			lease:     k("lease"),
		},
	}
}

// startScript starts a run with the token in ARGV[2], unless a run is finished
// or running with a live lease. It returns 1 on success, 0 if refused.
var startScript = redis.NewScript(`
local state = redis.call('GET', KEYS[1])
if state == 'finished' or (state == 'running' and redis.call('EXISTS', KEYS[10]) == 1) then
	return 0
end
redis.call('UNLINK', KEYS[3], KEYS[4], KEYS[5], KEYS[6], KEYS[7], KEYS[8])
redis.call('SET', KEYS[2], ARGV[1])
redis.call('SET', KEYS[9], ARGV[2])
redis.call('SET', KEYS[10], ARGV[2], 'PX', ARGV[3])
redis.call('SET', KEYS[1], 'running')
return 1
`)

func (r *Redis) TryStart(ctx context.Context, dedupMode string) (Run, error) {
	k := r.keys
	token := rand.Text()
	ok, err := startScript.Run(ctx, r.client,
		[]string{k.state, k.mode, k.processed, k.unique, k.skipped, k.seen, k.senders, k.err, k.owner, k.lease},
		dedupMode, token, LeaseTTL.Milliseconds()).Int()
	if err != nil {
		return nil, fmt.Errorf("start: %w", err)
	}
	if ok == 0 {
		return nil, ErrAlreadyStarted
	}
	return &redisRun{r: r, token: token}, nil
}

// redisRun is a Run whose token identifies it in the owner key. Its scripts
// only act while the run is in progress and still owned by the token.
type redisRun struct {
	r     *Redis
	token string
}

var _ Run = (*redisRun)(nil)

// renewScript sets the lease of the run with the token in ARGV[1] to ARGV[2] ms.
// It returns 1 on success, 0 if the run isn't in progress.
var renewScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) ~= 'running' or redis.call('GET', KEYS[2]) ~= ARGV[1] then
	return 0
end
redis.call('SET', KEYS[3], ARGV[1], 'PX', ARGV[2])
return 1
`)

func (run *redisRun) RenewLease(ctx context.Context) error {
	k := run.r.keys
	ok, err := renewScript.Run(ctx, run.r.client, []string{k.state, k.owner, k.lease},
		run.token, LeaseTTL.Milliseconds()).Int()
	if err != nil {
		return fmt.Errorf("renew lease: %w", err)
	}
	if ok == 0 {
		return ErrNotRunning
	}
	return nil
}

// recordScript counts a batch of records given as ARGV pairs (key, from)
// after the token in ARGV[1]. The whole batch is applied atomically, and only
// while the run is in progress. It returns 1 on success, 0 if it isn't.
var recordScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) ~= 'running' or redis.call('GET', KEYS[7]) ~= ARGV[1] then
	return 0
end
local unique, skipped = 0, 0
for i = 2, #ARGV, 2 do
	if redis.call('SADD', KEYS[5], ARGV[i]) == 1 then
		unique = unique + 1
		if ARGV[i + 1] == '' then
			skipped = skipped + 1
		else
			redis.call('ZINCRBY', KEYS[6], 1, ARGV[i + 1])
		end
	end
end
redis.call('INCRBY', KEYS[2], (#ARGV - 1) / 2)
redis.call('INCRBY', KEYS[3], unique)
redis.call('INCRBY', KEYS[4], skipped)
return 1
`)

func (run *redisRun) RecordBatch(ctx context.Context, records []Record) error {
	if len(records) == 0 {
		return nil
	}
	args := make([]any, 0, 1+2*len(records))
	args = append(args, run.token)
	for _, rec := range records {
		args = append(args, rec.Key, rec.From)
	}
	k := run.r.keys
	ok, err := recordScript.Run(ctx, run.r.client,
		[]string{k.state, k.processed, k.unique, k.skipped, k.seen, k.senders, k.owner},
		args...).Int()
	if err != nil {
		return fmt.Errorf("record batch: %w", err)
	}
	if ok == 0 {
		return ErrNotRunning
	}
	return nil
}

// endScript moves the run with the token in ARGV[3] to the state in ARGV[1]
// with the reason in ARGV[2], and drops its lease.
// It returns 1 on success, 0 if the run isn't in progress.
var endScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) ~= 'running' or redis.call('GET', KEYS[3]) ~= ARGV[3] then
	return 0
end
redis.call('SET', KEYS[1], ARGV[1])
redis.call('SET', KEYS[2], ARGV[2])
redis.call('DEL', KEYS[4])
return 1
`)

func (run *redisRun) Finish(ctx context.Context) error {
	return run.end(ctx, StateFinished, "")
}

func (run *redisRun) Fail(ctx context.Context, reason string) error {
	return run.end(ctx, StateFailed, reason)
}

func (run *redisRun) end(ctx context.Context, state State, reason string) error {
	k := run.r.keys
	ok, err := endScript.Run(ctx, run.r.client, []string{k.state, k.err, k.owner, k.lease},
		string(state), reason, run.token).Int()
	if err != nil {
		return fmt.Errorf("set state %s: %w", state, err)
	}
	if ok == 0 {
		return ErrNotRunning
	}
	return nil
}

func (r *Redis) Status(ctx context.Context) (Status, error) {
	k := r.keys
	// MGET reads all values at once, so the counts are consistent with each other.
	vals, err := r.client.MGet(ctx, k.state, k.mode, k.processed, k.unique, k.skipped, k.err).Result()
	if err != nil {
		return Status{}, fmt.Errorf("status: %w", err)
	}
	str := func(v any) string {
		s, _ := v.(string) // nil for missing keys
		return s
	}
	var errs []error
	num := func(name string, v any) int64 {
		s := str(v)
		if s == "" {
			return 0
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			errs = append(errs, fmt.Errorf("parse %s %q: %w", name, s, err))
		}
		return n
	}

	st := Status{
		State:     State(str(vals[0])),
		DedupMode: str(vals[1]),
		Processed: num("processed", vals[2]),
		Unique:    num("unique", vals[3]),
		Skipped:   num("skipped", vals[4]),
		Error:     str(vals[5]),
	}
	if st.State == "" {
		st.State = StateNotStarted
	}
	if err := errors.Join(errs...); err != nil {
		return Status{}, fmt.Errorf("status: %w", err)
	}
	return st, nil
}

func (r *Redis) TopSenders(ctx context.Context, n int) ([]SenderCount, error) {
	if n <= 0 {
		return []SenderCount{}, nil
	}
	// Senders with equal counts are returned in reverse alphabetical order (Redis's tie-break).
	zs, err := r.client.ZRevRangeWithScores(ctx, r.keys.senders, 0, int64(n-1)).Result()
	if err != nil {
		return nil, fmt.Errorf("top senders: %w", err)
	}
	out := make([]SenderCount, len(zs))
	for i, z := range zs {
		email, _ := z.Member.(string)
		out[i] = SenderCount{Email: email, Count: int64(z.Score)}
	}
	return out, nil
}
