package api

import (
	"context"
	stdjson "encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/artie-labs/ducktape/api/pkg/ducktape"
)

func TestAppendPreservesBigIntTokens(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "precision.db")
	_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
		Statements: []ducktape.ExecuteStatement{{Query: "CREATE TABLE numbers (id BIGINT)"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"9007199254740993", "-9007199254740993", "9223372036854775807", "-9223372036854775808", "9007199254740991", "9007199254740992", "42"}
	for _, token := range want {
		t.Run(token, func(t *testing.T) {
			_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{
				Statements: []ducktape.ExecuteStatement{{Query: "DELETE FROM numbers"}},
			})
			if err != nil {
				t.Fatal(err)
			}
			input := `{"rv":[` + token + "]}\n"
			rows, bytesRead, err := Append(ctx, dsn, "precision", "main", "numbers", strings.NewReader(input))
			if err != nil {
				t.Fatal(err)
			}
			if rows != 1 || bytesRead != uint64(len(input)-1) {
				t.Fatalf("unexpected append counts: rows=%d bytes=%d", rows, bytesRead)
			}
			result, err := Query(ctx, dsn, ducktape.QueryRequest{Query: "SELECT CAST(id AS VARCHAR) AS id FROM numbers"})
			if err != nil {
				t.Fatal(err)
			}
			if len(result) != 1 {
				t.Fatalf("expected one row, got %d", len(result))
			}
			if result[0]["id"] != token {
				t.Fatalf("stored %v, want exact integer %s", result[0]["id"], token)
			}
		})
	}
}

func TestAppendNumericCompatibility(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "compatibility.db")
	_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{Statements: []ducktape.ExecuteStatement{{Query: `CREATE TABLE numbers (
		i BIGINT, u UBIGINT, d DOUBLE, z DOUBLE, amount DECIMAL(20,2),
		ids BIGINT[], payload STRUCT(id BIGINT, ratio DOUBLE, children BIGINT[]), missing BIGINT
	)`}}})
	if err != nil {
		t.Fatal(err)
	}
	input := `{"rv":[42,18446744073709551615,1.25e2,-0,"9007199254740993.25",[42,null],{"id":42,"ratio":1.5,"children":[7,null]},null]}` + "\n"
	rows, bytesRead, err := Append(ctx, dsn, "compatibility", "main", "numbers", strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if rows != 1 || bytesRead != uint64(len(input)-1) {
		t.Fatalf("counts: %d %d", rows, bytesRead)
	}
	result, err := Query(ctx, dsn, ducktape.QueryRequest{Query: `SELECT
		i = 42 AS small, d = 125 AS exponent, signbit(z) AS negative_zero,
		amount::VARCHAR AS amount, ids[1] = 42 AND ids[2] IS NULL AS list_ok,
		payload.id = 42 AND payload.ratio = 1.5 AND payload.children[1] = 7 AND payload.children[2] IS NULL AS struct_ok,
		missing IS NULL AS null_ok FROM numbers`})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("rows: %#v", result)
	}
	for _, key := range []string{"small", "exponent", "negative_zero", "list_ok", "struct_ok", "null_ok"} {
		if result[0][key] != true {
			t.Errorf("%s: %#v", key, result[0])
		}
	}
	if result[0]["amount"] != "9007199254740993.25" {
		t.Errorf("decimal: %#v", result)
	}
}

func TestAppendNestedIntegerTokens(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "nested.db")
	_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{Statements: []ducktape.ExecuteStatement{{Query: `CREATE TABLE numbers (ids BIGINT[], payload STRUCT(id BIGINT, children UBIGINT[]))`}}})
	if err != nil {
		t.Fatal(err)
	}
	input := `{"rv":[[9007199254740993,null],{"id":-9007199254740993,"children":[18446744073709551615,null]}]}`
	_, _, err = Append(ctx, dsn, "nested", "main", "numbers", strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	result, err := Query(ctx, dsn, ducktape.QueryRequest{Query: `SELECT ids[1]::VARCHAR AS id, payload.id::VARCHAR AS negative, payload.children[1]::VARCHAR AS unsigned FROM numbers`})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 || result[0]["id"] != "9007199254740993" || result[0]["negative"] != "-9007199254740993" || result[0]["unsigned"] != "18446744073709551615" {
		t.Fatalf("stored: %#v", result)
	}
}

func TestAppendRejectsMalformedNumericRows(t *testing.T) {
	for _, input := range []string{`{"rv":[1]} {"rv":[2]}`, `{"rv":[1]}x`, `{"rv":[01]}`, `{"rv":[1e400]}`, `{"rv":[1`, `{"rv":[1]} null`, `{"rv":[00]}`, `{"rv":[-01]}`, `{"rv":[1.]}`, `{"rv":[.1]}`, `{"rv":[+1]}`, `{"rv":[NaN]}`, `{"rv":[Infinity]}`, `{"rv":[1],}`, `{"rv":[1]} /*x*/`, `{"rv":[9007199254740993]}x`} {
		t.Run(input, func(t *testing.T) {
			ctx := context.Background()
			dsn := filepath.Join(t.TempDir(), "malformed.db")
			_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{Statements: []ducktape.ExecuteStatement{{Query: "CREATE TABLE numbers (id DOUBLE)"}}})
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := Append(ctx, dsn, "malformed", "main", "numbers", strings.NewReader(input)); err == nil {
				t.Fatal("accepted malformed row")
			} else if !strings.HasPrefix(err.Error(), "failed to unmarshal row message for database malformed, schema main, table numbers:") {
				t.Fatalf("lost row error context: %v", err)
			}
		})
	}
}

func TestAppendDoubleTokens(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "floating.db")
	_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{Statements: []ducktape.ExecuteStatement{{Query: "CREATE TABLE numbers (value DOUBLE)"}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, token := range []string{"9007199254740993", "18446744073709551615", "18446744073709551616", "-0", "-0.0", "1.25", "1e3", "1e-300"} {
		t.Run(token, func(t *testing.T) {
			_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{Statements: []ducktape.ExecuteStatement{{Query: "DELETE FROM numbers"}}})
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := Append(ctx, dsn, "floating", "main", "numbers", strings.NewReader(`{"rv":[`+token+`]}`)); err != nil {
				t.Fatal(err)
			}
			result, err := Query(ctx, dsn, ducktape.QueryRequest{Query: "SELECT value FROM numbers"})
			if err != nil {
				t.Fatal(err)
			}
			want, err := strconv.ParseFloat(token, 64)
			if err != nil {
				t.Fatal(err)
			}
			if len(result) != 1 {
				t.Fatalf("rows: %#v", result)
			}
			got, ok := result[0]["value"].(float64)
			if !ok || got != want || math.Signbit(got) != math.Signbit(want) {
				t.Fatalf("stored %v, want %v (signed zero preserved)", result, want)
			}
		})
	}
}

// TestAppendIntegerBoundaries exercises valid target domains without blessing
// the pinned driver's unchecked casts for values outside those domains.
func TestAppendIntegerBoundaries(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "boundaries.db")
	_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{Statements: []ducktape.ExecuteStatement{{Query: `CREATE TABLE numbers (i BIGINT, u UBIGINT, ids BIGINT[], payload STRUCT(ids UBIGINT[]), d DOUBLE)`}}})
	if err != nil {
		t.Fatal(err)
	}
	// Alternate exact and ordinary rows, including surrounding whitespace and
	// nulls, to catch accidental reuse or retention of earlier row values.
	input := "  " + `{"rv":[-9223372036854775808,0,[-9223372036854775808,9223372036854775807],{"ids":[0,18446744073709551615]},9007199254740993]}` + " \n" +
		`{"rv":[42,42,[42],{"ids":[42]},-0]}` + "\n" +
		`{"rv":[9223372036854775807,18446744073709551615,null,null,18446744073709551615]}` + "\n" +
		`{"rv":[null,null,null,null,null]}` + "\n"
	if n, _, err := Append(ctx, dsn, "boundaries", "main", "numbers", strings.NewReader(input)); err != nil || n != 4 {
		t.Fatalf("rows=%d err=%v", n, err)
	}
	result, err := Query(ctx, dsn, ducktape.QueryRequest{Query: `SELECT
 count(*) = 4 AS rows_ok,
 count(*) FILTER (WHERE i = '-9223372036854775808'::BIGINT AND u = 0 AND ids[1] = '-9223372036854775808'::BIGINT AND ids[2] = '9223372036854775807'::BIGINT AND payload.ids[2] = '18446744073709551615'::UBIGINT AND d = '9007199254740992'::DOUBLE) = 1 AS minimum_ok,
 count(*) FILTER (WHERE i = '9223372036854775807'::BIGINT AND u = '18446744073709551615'::UBIGINT AND d = '18446744073709551616'::DOUBLE) = 1 AS maximum_ok,
 count(*) FILTER (WHERE i = 42 AND signbit(d)) = 1 AS ordinary_ok,
 count(*) FILTER (WHERE i IS NULL AND u IS NULL) = 1 AS null_ok FROM numbers`})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("rows: %#v", result)
	}
	for _, key := range []string{"rows_ok", "minimum_ok", "maximum_ok", "ordinary_ok", "null_ok"} {
		if result[0][key] != true {
			t.Errorf("%s: %#v", key, result)
		}
	}
}

// TestAppendExactRowsWithVariablePayloads protects decoder buffering across
// selected rows and checks that long digit strings remain strings.
func TestAppendExactRowsWithVariablePayloads(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "buffering.db")
	_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{Statements: []ducktape.ExecuteStatement{{Query: "CREATE TABLE numbers (id BIGINT, label VARCHAR)"}}})
	if err != nil {
		t.Fatal(err)
	}
	label := strings.Repeat("x", 10000) + "1234567890123456"
	input := ` {"rv":[9007199254740993,"` + label + `"]} ` + "\n" +
		`{"rv":[42,"ordinary"]}` + "\n" +
		`{"rv":[-9007199254740993,"short"]}` + "\n" +
		`{"rv":[7,"1234567890123456"]}` + "\n"
	if n, _, err := Append(ctx, dsn, "buffering", "main", "numbers", strings.NewReader(input)); err != nil || n != 4 {
		t.Fatalf("rows=%d err=%v", n, err)
	}
	result, err := Query(ctx, dsn, ducktape.QueryRequest{Query: "SELECT id::VARCHAR AS id, label FROM numbers ORDER BY numbers.id"})
	if err != nil {
		t.Fatal(err)
	}
	wantIDs := []string{"-9007199254740993", "7", "42", "9007199254740993"}
	wantLabels := []string{"short", "1234567890123456", "ordinary", label}
	if len(result) != len(wantIDs) {
		t.Fatalf("rows: %#v", result)
	}
	for i, row := range result {
		if row["id"] != wantIDs[i] || row["label"] != wantLabels[i] {
			t.Fatalf("row %d: %#v", i, row)
		}
	}
}

// TestAppendQuotedPayloadTransitions guards quote and backslash parity while
// switching decoders, so unrelated text cannot hide a following integer token.
func TestAppendQuotedPayloadTransitions(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "quoted.db")
	_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{Statements: []ducktape.ExecuteStatement{{Query: "CREATE TABLE numbers (seq BIGINT, label VARCHAR, id BIGINT, payload STRUCT(ids BIGINT[]))"}}})
	if err != nil {
		t.Fatal(err)
	}
	var input strings.Builder
	var labels []string
	var ids []string
	for i, n := range []int{0, 1, 511, 512, 513, 4096, 65535, 2, 100000, 3} {
		// Marshaling gives real escaped quotes, odd/even backslashes and digit runs.
		label := strings.Repeat("x", n) + `1234567890123456\"1234567890123456\\` + "\"tail\\"
		encoded, err := stdjson.Marshal(label)
		if err != nil {
			t.Fatal(err)
		}
		id := "42"
		if i%2 == 0 {
			id = "9007199254740993"
		}
		if i%3 == 0 {
			id = "-9007199254740993"
		}
		fmt.Fprintf(&input, "  {\"rv\":[%d,%s,%s,{\"ids\":[%s,null]}]}  \n", i, encoded, id, id)
		labels = append(labels, label)
		ids = append(ids, id)
	}
	// Unicode-escaped digits and an escaped quote immediately before a large ID.
	input.WriteString(`{"rv":[10,"\u0031\u0032\u0033\"\\",9007199254740993,{"ids":[9007199254740993,null]}]}` + "\n")
	labels = append(labels, "123\"\\")
	ids = append(ids, "9007199254740993")
	if n, _, err := Append(ctx, dsn, "quoted", "main", "numbers", strings.NewReader(input.String())); err != nil || n != int64(len(ids)) {
		t.Fatalf("n=%d err=%v", n, err)
	}
	got, err := Query(ctx, dsn, ducktape.QueryRequest{Query: "SELECT label,id::VARCHAR AS id,payload.ids[1]::VARCHAR AS nested,payload.ids[2] IS NULL AS null_ok FROM numbers ORDER BY seq"})
	if err != nil || len(got) != len(ids) {
		t.Fatalf("rows=%v err=%v", got, err)
	}
	for i, row := range got {
		if row["label"] != labels[i] || row["id"] != ids[i] || row["nested"] != ids[i] || row["null_ok"] != true {
			t.Fatalf("row %d: %#v", i, row)
		}
	}
}

// TestAppendNormalizationErrorContext preserves row location even when decode
// succeeds and conversion rejects an overflowing floating-point value.
func TestAppendNormalizationErrorContext(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "context.db")
	_, err := Execute(ctx, dsn, ducktape.ExecuteRequest{Statements: []ducktape.ExecuteStatement{{Query: "CREATE TABLE numbers (id BIGINT, d DOUBLE, label VARCHAR)"}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"42", "9007199254740993"} {
		for _, tail := range []string{"", " null", "x", " /*comment*/"} {
			_, _, err := Append(ctx, dsn, "context", "main", "numbers", strings.NewReader(`{"rv":[`+id+`,1e400,"1234567890123456\"\\"]}`+tail))
			if err == nil || !strings.HasPrefix(err.Error(), "failed to unmarshal row message for database context, schema main, table numbers:") {
				t.Fatalf("id=%s tail=%q err=%v", id, tail, err)
			}
		}
	}
}
