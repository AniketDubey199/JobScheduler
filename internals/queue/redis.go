package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const DefaultQueue = "queue:default" // this is key for the redis

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
			return "", errors.New("queue is empty")
		}
		return "", fmt.Errorf("failed to dequeue job %w", err)
	}
	return results[1], nil
}

func (c *Client) Close() error {
	return c.rdb.Close()
}
