package scrape

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// bosun types

type BosunResponse struct {
	Type    string
	Results []*Result
	Queries map[string]Request
}

type Result struct {
	Computations
	Value ResultValue // Value 类型 Number/Series 两种
	Group TagSet
}

type ResultValue struct {
	Number *Number
	Series *Series
}

func (r *ResultValue) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		return nil
	} else if data[0] == '{' {
		return json.Unmarshal(data, r.Series)
	} else {
		v, err := strconv.ParseFloat(string(data), 10)
		if err != nil {
			return err
		}
		r.Number = &v
		return nil
	}
}

type Value interface {
	Type() FuncType
	Value() interface{}
}

type Number = float64

type Scalar float64

func (s Scalar) Type() FuncType               { return TypeScalar }
func (s Scalar) Value() interface{}           { return s }
func (s Scalar) MarshalJSON() ([]byte, error) { return marshalFloat(float64(s)) }

type String string

func (s String) Type() FuncType     { return TypeString }
func (s String) Value() interface{} { return s }

type Info []interface{}

func (i Info) Type() FuncType     { return TypeInfo }
func (i Info) Value() interface{} { return i }

//func (s String) MarshalJSON() ([]byte, error) { return json.Marshal(s) }

// Series is the standard form within bosun to represent timeseries data.
type Series map[int64]float64

func (a Series) Equal(b Series) bool {
	return reflect.DeepEqual(a, b)
}

type Computations []Computation

type Computation struct {
	Text  string
	Value interface{}
}

type FuncType int

func (f FuncType) String() string {
	switch f {
	case TypeNumberSet:
		return "number"
	case TypeString:
		return "string"
	case TypeSeriesSet:
		return "series"
	case TypeScalar:
		return "scalar"
	case TypeESQuery:
		return "esquery"
	case TypeESIndexer:
		return "esindexer"
	case TypeNumberExpr:
		return "numberexpr"
	case TypeSeriesExpr:
		return "seriesexpr"
	case TypePrefix:
		return "prefix"
	case TypeTable:
		return "table"
	case TypeVariantSet:
		return "variantSet"
	case TypeAzureResourceList:
		return "azureResources"
	case TypeAzureAIApps:
		return "azureAIApps"
	case TypeInfo:
		return "info"
	default:
		return "unknown"
	}
}

const (
	TypeString FuncType = iota
	TypePrefix
	TypeScalar
	TypeNumberSet
	TypeSeriesSet
	TypeESQuery
	TypeESIndexer
	TypeNumberExpr
	TypeSeriesExpr // No implementation yet
	TypeTable
	TypeVariantSet
	TypeAzureResourceList
	TypeAzureAIApps
	TypeInfo
	TypeUnexpected
)

func marshalFloat(n float64) ([]byte, error) {
	if math.IsNaN(n) {
		return json.Marshal("NaN")
	} else if math.IsInf(n, 1) {
		return json.Marshal("+Inf")
	} else if math.IsInf(n, -1) {
		return json.Marshal("-Inf")
	}
	return json.Marshal(n)
}

// TagSet is a helper class for tags.
type TagSet map[string]string

// Copy creates a new TagSet from t.
func (t TagSet) Copy() TagSet {
	n := make(TagSet)
	for k, v := range t {
		n[k] = v
	}
	return n
}

// Merge adds or overwrites everything from o into t and returns t.
func (t TagSet) Merge(o TagSet) TagSet {
	for k, v := range o {
		t[k] = v
	}
	return t
}

// Equal returns true if t and o contain only the same k=v pairs.
func (t TagSet) Equal(o TagSet) bool {
	if len(t) != len(o) {
		return false
	}
	for k, v := range t {
		if ov, ok := o[k]; !ok || ov != v {
			return false
		}
	}
	return true
}

// Subset returns true if all k=v pairs in o are in t.
func (t TagSet) Subset(o TagSet) bool {
	if len(o) > len(t) {
		return false
	}
	for k, v := range o {
		if tv, ok := t[k]; !ok || tv != v {
			return false
		}
	}
	return true
}

// Compatible returns true if all keys that are in both o and t, have the same value.
func (t TagSet) Compatible(o TagSet) bool {
	for k, v := range o {
		if tv, ok := t[k]; ok && tv != v {
			return false
		}
	}
	return true
}

// Intersection returns the intersection of t and o.
func (t TagSet) Intersection(o TagSet) TagSet {
	r := make(TagSet)
	for k, v := range t {
		if o[k] == v {
			r[k] = v
		}
	}
	return r
}

// String converts t to an OpenTSDB-style {a=b,c=b} string, alphabetized by key.
func (t TagSet) String() string {
	return fmt.Sprintf("{%s}", t.Tags())
}

// Tags is identical to String() but without { and }.
func (t TagSet) Tags() string {
	var keys []string
	for k := range t {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	b := &bytes.Buffer{}
	for i, k := range keys {
		if i > 0 {
			fmt.Fprint(b, ",")
		}
		fmt.Fprintf(b, "%s=%s", k, t[k])
	}
	return b.String()
}

func (t TagSet) AllSubsets() []string {
	var keys []string
	for k := range t {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return t.allSubsets("", 0, keys)
}

func (t TagSet) allSubsets(base string, start int, keys []string) []string {
	subs := []string{}
	for i := start; i < len(keys); i++ {
		part := base
		if part != "" {
			part += ","
		}
		part += fmt.Sprintf("%s=%s", keys[i], t[keys[i]])
		subs = append(subs, part)
		subs = append(subs, t.allSubsets(part, i+1, keys)...)
	}
	return subs
}

// Returns true if the two tagsets "overlap".
// Two tagsets overlap if they:
// 1. Have at least one key/value pair that matches
// 2. Have no keys in common where the values do not match
func (a TagSet) Overlaps(b TagSet) bool {
	anyMatch := false
	for k, v := range a {
		v2, ok := b[k]
		if !ok {
			continue
		}
		if v2 != v {
			return false
		}
		anyMatch = true
	}
	return anyMatch
}

var bigMaxInt64 = big.NewInt(math.MaxInt64)

// Request holds query objects:
// http://opentsdb.net/docs/build/html/api_http/query/index.html#requests.
type Request struct {
	Start             interface{} `json:"start"`
	End               interface{} `json:"end,omitempty"`
	Queries           []*Query    `json:"queries"`
	NoAnnotations     bool        `json:"noAnnotations,omitempty"`
	GlobalAnnotations bool        `json:"globalAnnotations,omitempty"`
	MsResolution      bool        `json:"msResolution,omitempty"`
	ShowTSUIDs        bool        `json:"showTSUIDs,omitempty"`
	Delete            bool        `json:"delete,omitempty"`
}

// Query is a query for a request:
// http://opentsdb.net/docs/build/html/api_http/query/index.html#sub-queries.
type Query struct {
	Aggregator  string      `json:"aggregator"`
	Metric      string      `json:"metric"`
	Rate        bool        `json:"rate,omitempty"`
	RateOptions RateOptions `json:"rateOptions,omitempty"`
	Downsample  string      `json:"downsample,omitempty"`
	Tags        TagSet      `json:"tags,omitempty"`
	Filters     Filters     `json:"filters,omitempty"`
	GroupByTags TagSet      `json:"-"`
}

type Filter struct {
	Type    string `json:"type"`
	TagK    string `json:"tagk"`
	Filter  string `json:"filter"`
	GroupBy bool   `json:"groupBy"`
}

func (f Filter) String() string {
	return fmt.Sprintf("%s=%s(%s)", f.TagK, f.Type, f.Filter)
}

type Filters []Filter

func (filters Filters) String() string {
	s := ""
	gb := make(Filters, 0)
	nGb := make(Filters, 0)
	for _, filter := range filters {
		if filter.GroupBy {
			gb = append(gb, filter)
			continue
		}
		nGb = append(nGb, filter)
	}
	s += "{"
	for i, filter := range gb {
		s += filter.String()
		if i != len(gb)-1 {
			s += ","
		}
	}
	s += "}"
	for i, filter := range nGb {
		if i == 0 {
			s += "{"
		}
		s += filter.String()
		if i == len(nGb)-1 {
			s += "}"
		} else {
			s += ","
		}
	}
	return s
}

// RateOptions are rate options for a query.
type RateOptions struct {
	Counter    bool  `json:"counter,omitempty"`
	CounterMax int64 `json:"counterMax,omitempty"`
	ResetValue int64 `json:"resetValue,omitempty"`
	DropResets bool  `json:"dropResets,omitempty"`
}

var filterValueRe = regexp.MustCompile(`([a-z_]+)\((.*)\)$`)

// ParseFilters parses filters in the form of `tagk=filterFunc(...),...`
// It also mimics OpenTSDB's promotion of queries with a * or no
// function to iwildcard and literal_or respectively
func ParseFilters(rawFilters string, grouping bool, q *Query) ([]Filter, error) {
	var filters []Filter
	for _, rawFilter := range strings.Split(rawFilters, ",") {
		splitRawFilter := strings.SplitN(rawFilter, "=", 2)
		if len(splitRawFilter) != 2 {
			return nil, fmt.Errorf("opentsdb: bad filter format: %s", rawFilter)
		}
		filter := Filter{}
		filter.TagK = splitRawFilter[0]
		if grouping {
			q.GroupByTags[filter.TagK] = ""
		}
		// See if we have a filter function, if not we have to use legacy parsing defined in
		// filter conversions of http://opentsdb.net/docs/build/html/api_http/query/index.html
		m := filterValueRe.FindStringSubmatch(splitRawFilter[1])
		if m != nil {
			filter.Type = m[1]
			filter.Filter = m[2]
		} else {
			// Legacy Conversion
			filter.Type = "literal_or"
			if strings.Contains(splitRawFilter[1], "*") {
				filter.Type = "iwildcard"
			}
			if splitRawFilter[1] == "*" {
				filter.Type = "wildcard"
			}
			filter.Filter = splitRawFilter[1]
		}
		filter.GroupBy = grouping
		filters = append(filters, filter)
	}
	return filters, nil
}

func (q Query) String() string {
	s := q.Aggregator + ":"
	if q.Downsample != "" {
		s += q.Downsample + ":"
	}
	if q.Rate {
		s += "rate"
		if q.RateOptions.Counter {
			s += "{"
			if q.RateOptions.DropResets {
				s += "dropcounter"
			} else {
				s += "counter"
			}
			if q.RateOptions.CounterMax != 0 {
				s += ","
				s += strconv.FormatInt(q.RateOptions.CounterMax, 10)
			}
			if q.RateOptions.ResetValue != 0 {
				if q.RateOptions.CounterMax == 0 {
					s += ","
				}
				s += ","
				s += strconv.FormatInt(q.RateOptions.ResetValue, 10)
			}
			s += "}"
		}
		s += ":"
	}
	s += q.Metric
	if len(q.Tags) > 0 {
		s += q.Tags.String()
	}
	if len(q.Filters) > 0 {
		s += q.Filters.String()
	}
	return s
}
