package bridge

import (
	"context"
	"log/slog"
)

// ponytail: slog.DiscardHandler needs Go 1.24; swap to it when go.mod bumps.
type discardHandler struct{}

func (discardHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (h discardHandler) WithAttrs([]slog.Attr) slog.Handler      { return h }
func (h discardHandler) WithGroup(string) slog.Handler           { return h }

func NopLogger() *slog.Logger { return slog.New(discardHandler{}) }
