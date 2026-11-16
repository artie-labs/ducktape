package api

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"

	zerobus "github.com/databricks/zerobus-sdk/go"

	"github.com/artie-labs/ducktape/api/pkg/ducktape"
)

func handleZerobusCheck(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	target, ok := parseZerobusTarget(w, r)
	if !ok {
		return
	}

	if err := ZerobusCheck(target); err != nil {
		errMsg := err.Error()
		handleInternalServerErrorJSON(w, ducktape.ZerobusCheckResponse{Error: &errMsg}, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("{}"))
	slog.Info(fmt.Sprintf("zerobus check passed for table %s", target.table), slog.Duration("elapsed", time.Since(start)))
}

// ZerobusCheck opens a Zerobus stream for the target table and closes it without sending a row. Opening one exchanges
// the client secret for a token and has Zerobus check the service principal can write to the table, so it fails on a
// wrong endpoint, a bad secret, a missing table or missing permissions.
func ZerobusCheck(target zerobusTarget) error {
	sdk, err := zerobus.NewZerobusSdkWithOptions(target.endpoint, target.workspaceURL, zerobus.WithApplicationName("artie-ducktape"))
	if err != nil {
		return fmt.Errorf("failed to create a zerobus client: %w", err)
	}
	defer sdk.Free()

	options := zerobus.DefaultStreamConfigurationOptions()
	options.RecordType = zerobus.RecordTypeJson
	options.FlushTimeoutMs = uint64(zerobusFlushTimeoutMs)
	stream, err := sdk.CreateStream(zerobus.TableProperties{TableName: target.table}, target.clientID, target.clientSecret, options)
	if err != nil {
		return fmt.Errorf("failed to open a zerobus stream for table %s: %w", target.table, err)
	}
	if err := stream.Close(); err != nil {
		return fmt.Errorf("failed to close the zerobus stream for table %s: %w", target.table, err)
	}
	return nil
}
