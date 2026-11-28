package queue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrQueueEmpty = errors.New("queue is empty")

const (
	DefaultQueue = "queue:default"
	RetryQueue   = "queue:retry"
	DLQQueue     = "queue:dlq"
)

// this is key for the redis
type QueueStats struct {
	Default int64 `json:"default"`
	Retry   int64 `json:"retry"`
	DLQ     int64 `json:"dlq"`
}

type Client struct {
	rdb *redis.Client
}

func NewClient(ctx context.Context, addr string) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "",
		DB:       0,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}
	return &Client{rdb: rdb}, nil
}

func (c *Client) Enqueue(ctx context.Context, jobID string) error {
	return c.rdb.LPush(ctx, DefaultQueue, jobID).Err()
}

func (c *Client) Dequeue(ctx context.Context, timeout time.Duration) (string, error) {
	results, err := c.rdb.BRPop(ctx, timeout, DefaultQueue).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", ErrQueueEmpty
		}
		return "", fmt.Errorf("failed to dequeue job %w", err)
	}
	return results[1], nil
}

func (c *Client) ScheduledRetry(ctx context.Context, jobID string, delay time.Duration) error {
	retryAt := float64(time.Now().Add(delay).Unix())
	err := c.rdb.ZAdd(ctx, RetryQueue, redis.Z{
		Score:  retryAt,
		Member: jobID,
	}).Err()
	if err != nil {
		return fmt.Errorf("failed to schedule retry for job %s: %w", jobID, err)
	}
	return nil
}

func (c *Client) GetQueueStats(ctx context.Context) (QueueStats, error) {
	pipe := c.rdb.Pipeline()
	defaultlen := pipe.LLen(ctx, DefaultQueue)
	retrylen := pipe.ZCard(ctx, RetryQueue)
	dlqlen := pipe.LLen(ctx, DLQQueue)

	if _, err := pipe.Exec(ctx); err != nil {
		return QueueStats{}, fmt.Errorf("failed to fetch queue stats: %w", err)
	}

	return QueueStats{
		Default: defaultlen.Val(),
		Retry:   retrylen.Val(),
		DLQ:     dlqlen.Val(),
	}, nil
}

func (c *Client) MovetoDQL(ctx context.Context, jobID string) error {
	if err := c.rdb.LPush(ctx, DLQQueue, jobID).Err(); err != nil {
		return fmt.Errorf("failed to move job %s to DLQ: %w", jobID, err)
	}
	return nil
}

func (c *Client) RemovefromDQL(ctx context.Context, jobID string) error {
	if err := c.rdb.LRem(ctx, DLQQueue, 0, jobID).Err(); err != nil {
		return fmt.Errorf("failed to remove job %s from DLQ: %w", jobID, err)
	}
	return nil
}

func (c *Client) FetchRetryEntries(ctx context.Context, limit int64) ([]string, error) {
	now := strconv.FormatInt(time.Now().Unix(), 10)

	jobID, err := c.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     RetryQueue,
		Start:   "-inf",
		Stop:    now,
		ByScore: true,
		Count:   limit,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch due retries: %w", err)
	}
	if len(jobID) == 0 {
		return nil, nil
	}
	// remove them from the ZSet
	members := make([]interface{}, len(jobID))
	for i, id := range jobID {
		members[i] = id
	}
	if err := c.rdb.ZRem(ctx, RetryQueue, members...).Err(); err != nil {
		return nil, fmt.Errorf("failed to remove due retries from ZSET: %w", err)
	}
	return jobID, nil
}
func (c *Client) Close() error {
	return c.rdb.Close()
}
