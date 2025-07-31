//go:build mage
// +build mage

package main

import (
	"context"

	//mage:import sql
	sql "github.com/ttab/mage/sql"
	//mage:import twirp
	_ "github.com/ttab/mage/twirp"
)

var reportingTables = []string{
	"state",
}

func GrantReporting(ctx context.Context) error {
	return sql.GrantReporting(ctx, reportingTables)
}
