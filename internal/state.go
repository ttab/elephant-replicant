package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/ttab/elephant-replicant/postgres"
)

func LoadState[T any](
	ctx context.Context,
	q *postgres.Queries,
	name string,
	state *T,
) error {
	data, err := q.GetState(ctx, name)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	} else if err != nil {
		return fmt.Errorf("read from database: %w", err)
	}

	err = json.Unmarshal(data, &state)
	if err != nil {
		return fmt.Errorf("unmarshal state: %w", err)
	}

	return nil
}

func StoreState[T any](
	ctx context.Context,
	q *postgres.Queries,
	name string,
	value T,
) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}

	err = q.SetState(ctx, postgres.SetStateParams{
		Name:  name,
		Value: data,
	})
	if err != nil {
		return fmt.Errorf("write to database: %w", err)
	}

	return nil
}
