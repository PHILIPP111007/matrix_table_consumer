package functions_go

import (
	"io"
	"time"
)

type VCFRow struct {
	Chrom      string
	ID         string
	Ref        string
	Alt        string
	Filter     string
	Info       string
	Format     string
	Samples    []string
	InfoFields map[string]string
	Pos        int32
	Qual       int8
}

type VCFRowJSON struct {
	Chrom  string `json:"CHROM"`
	Id     string `json:"ID"`
	Ref    string `json:"REF"`
	Alt    string `json:"ALT"`
	Filter string `json:"FILTER"`
	Info   string `json:"INFO"`
	Pos    int32  `json:"POS"`
	Qual   int8   `json:"QUAL"`
}

type Rows []*VCFRowJSON

type VCFRecordWithSamples struct {
	Chrom   string
	Pos     string
	ID      string
	Ref     string
	Alt     string
	Qual    string
	Filter  string
	Info    string
	Format  string
	Samples map[string]string
}

// Tqdm introduces progress bar
type Tqdm struct {
	startTime   time.Time
	lastUpdate  time.Time
	description string
	barFormat   string
	unit        string
	writer      io.Writer

	total       int
	current     int
	width       int
	minInterval time.Duration

	showBar   bool
	showRate  bool
	showETA   bool
	unitScale bool
}

// Option defines a function to configure Tqdm
type Option func(*Tqdm)
