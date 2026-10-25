package main

import (
	"strings"
	"testing"

	"github.com/wilds-prosy9x/enron-ingestion/internal/ingest"
)

func env(vars map[string]string) func(string) string {
	return func(name string) string { return vars[name] }
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := loadConfig(env(map[string]string{"DATASET_BUCKET": "file:///data"}))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	want := config{
		addr:          ":8080",
		datasetBucket: "file:///data",
		datasetKey:    "enron_mail_20150507.tar.gz",
		dedupMode:     ingest.DedupContent,
	}
	if cfg != want {
		t.Errorf("config = %+v, want %+v", cfg, want)
	}
}

func TestLoadConfigOverrides(t *testing.T) {
	cfg, err := loadConfig(env(map[string]string{
		"ADDR":           ":9000",
		"DATASET_BUCKET": "s3://bucket?region=eu-north-1",
		"DATASET_KEY":    "other.tar.gz",
		"DEDUP_MODE":     "message_id",
	}))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	want := config{
		addr:          ":9000",
		datasetBucket: "s3://bucket?region=eu-north-1",
		datasetKey:    "other.tar.gz",
		dedupMode:     ingest.DedupMessageID,
	}
	if cfg != want {
		t.Errorf("config = %+v, want %+v", cfg, want)
	}
}

func TestLoadConfigErrors(t *testing.T) {
	tests := []struct {
		name    string
		vars    map[string]string
		wantErr string
	}{
		{"missing bucket", map[string]string{}, "DATASET_BUCKET"},
		{"unknown dedup mode", map[string]string{"DATASET_BUCKET": "file:///data", "DEDUP_MODE": "subject"}, "DEDUP_MODE"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadConfig(env(tt.vars))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("err = %v, want an error mentioning %s", err, tt.wantErr)
			}
		})
	}
}
