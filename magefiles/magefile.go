//go:build mage
// +build mage

package main

import (
	"context"

	"github.com/ttab/elephant-replicant/schema"

	//mage:import sql
	sql "github.com/ttab/mage/sql"
	//mage:import twirp
	_ "github.com/ttab/mage/twirp"
)

func GrantReporting(ctx context.Context) error {
	return sql.GrantReportingFromJSON(ctx, schema.ReportingTables)
}
