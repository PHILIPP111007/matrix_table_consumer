package functions_go

import (
	"io"
	"time"
)

type VCFContainer struct {
	Chrom  string `json:"CHROM"`
	Id     string `json:"ID"`
	Ref    string `json:"REF"`
	Alt    string `json:"ALT"`
	Filter string `json:"FILTER"`
	Info   string `json:"INFO"`
	Pos    int32  `json:"POS"`
	Qual   int8   `json:"QUAL"`
}

type Rows []*VCFContainer

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
