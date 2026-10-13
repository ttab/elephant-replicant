//go:build mage
// +build mage

package main

import (
	"context"

	"github.com/ttab/elephant-replicant/schema"

	//mage:import sql
	sql "github.com/ttab/mage/sql"
)

func GrantReporting(ctx context.Context) error {
	return sql.GrantReportingFromJSON(ctx, schema.ReportingTables)
}
