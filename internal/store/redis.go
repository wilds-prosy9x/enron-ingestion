package store

import (
	"context"
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
		},
	}
}

// startScript starts a run unless one is running or finished.
// It returns 1 on success, 0 if refused.
var startScript = redis.NewScript(`
local state = redis.call('GET', KEYS[1])
if state == 'running' or state == 'finished' then
	return 0
end
redis.call('UNLINK', KEYS[3], KEYS[4], KEYS[5], KEYS[6], KEYS[7], KEYS[8])
redis.call('SET', KEYS[2], ARGV[1])
redis.call('SET', KEYS[1], 'running')
return 1
`)

func (r *Redis) TryStart(ctx context.Context, dedupMode string) error {
	k := r.keys
	ok, err := startScript.Run(ctx, r.client,
		[]string{k.state, k.mode, k.processed, k.unique, k.skipped, k.seen, k.senders, k.err},
		dedupMode).Int()
	if err != nil {
		return fmt.Errorf("start: %w", err)
	}
	if ok == 0 {
		return ErrAlreadyStarted
	}
	return nil
}

// recordScript counts a batch of records given as ARGV pairs (key, from).
// The whole batch is applied atomically, and only while the run is in progress.
// It returns 1 on success, 0 if no run is in progress.
var recordScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) ~= 'running' then
	return 0
end
local unique, skipped = 0, 0
for i = 1, #ARGV, 2 do
	if redis.call('SADD', KEYS[5], ARGV[i]) == 1 then
		unique = unique + 1
		if ARGV[i + 1] == '' then
			skipped = skipped + 1
		else
			redis.call('ZINCRBY', KEYS[6], 1, ARGV[i + 1])
		end
	end
end
redis.call('INCRBY', KEYS[2], #ARGV / 2)
redis.call('INCRBY', KEYS[3], unique)
redis.call('INCRBY', KEYS[4], skipped)
return 1
`)

func (r *Redis) RecordBatch(ctx context.Context, records []Record) error {
	if len(records) == 0 {
		return nil
	}
	args := make([]any, 0, 2*len(records))
	for _, rec := range records {
		args = append(args, rec.Key, rec.From)
	}
	k := r.keys
	ok, err := recordScript.Run(ctx, r.client,
		[]string{k.state, k.processed, k.unique, k.skipped, k.seen, k.senders},
		args...).Int()
	if err != nil {
		return fmt.Errorf("record batch: %w", err)
	}
	if ok == 0 {
		return ErrNotRunning
	}
	return nil
}

// endScript moves a running run to the state in ARGV[1] with the reason in ARGV[2].
// It returns 1 on success, 0 if no run is in progress.
var endScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) ~= 'running' then
	return 0
end
redis.call('SET', KEYS[1], ARGV[1])
redis.call('SET', KEYS[2], ARGV[2])
return 1
`)

func (r *Redis) Finish(ctx context.Context) error {
	return r.end(ctx, StateFinished, "")
}

func (r *Redis) Fail(ctx context.Context, reason string) error {
	return r.end(ctx, StateFailed, reason)
}

func (r *Redis) end(ctx context.Context, state State, reason string) error {
	ok, err := endScript.Run(ctx, r.client, []string{r.keys.state, r.keys.err}, string(state), reason).Int()
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
