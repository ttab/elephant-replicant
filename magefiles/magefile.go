//go:build mage
// +build mage

package main

import (
	"context"

	"github.com/ttab/elephant-replicant/schema"
	//mage:import docs
	_ "github.com/ttab/mage/docs"
	//mage:import sql
	sql "github.com/ttab/mage/sql"
)

func GrantReporting(ctx context.Context) error {
	return sql.GrantReportingFromJSON(ctx, schema.ReportingTables)
}
