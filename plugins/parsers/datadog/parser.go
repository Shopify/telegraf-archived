package datadog

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/smithy-go/time"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
)

type Config struct {
}

type Parser struct {
	defaultTags map[string]string
}

type Point struct {
	Ts    float64
	Value float64
}

type Serie struct {
	Name           string      `json:"metric"`
	Points         [][]float64 `json:"points"`
	Tags           []string    `json:"tags"`
	Host           string      `json:"host"`
	MType          string      `json:"type"`
	Interval       int64       `json:"interval"`
	SourceTypeName string      `json:"source_type_name,omitempty"`
}

type DataDog struct {
	Series []Serie `json:"series"`
}

func New() (*Parser, error) {
	return &Parser{}, nil
}

func (p *Parser) Parse(buf []byte) ([]telegraf.Metric, error) {
	var dataDog DataDog
	if err := json.Unmarshal(buf, &dataDog); err != nil {
		return nil, err
	}
	result := []telegraf.Metric{}
	for _, serie := range dataDog.Series {
		tags := splitTags(serie.Tags)
		fields := map[string]interface{}{
			"value": serie.Points[0][1],
		}
		timestamp := time.ParseEpochSeconds(serie.Points[0][0])
		metric, err := metric.New(serie.Name, tags, fields, timestamp)
		if err != nil {
			return nil, err
		}
		result = append(result, metric)
	}
	return result, nil
}

func (p *Parser) ParseLine(line string) (telegraf.Metric, error) {
	metrics, err := p.Parse([]byte(line + "\n"))

	if err != nil {
		return nil, err
	}

	if len(metrics) < 1 {
		return nil, fmt.Errorf("can not parse the line: %s, for data format: json ", line)
	}

	return metrics[0], nil
}

func (p *Parser) SetDefaultTags(tags map[string]string) {
	p.defaultTags = tags
}

func splitTags(rawTags []string) map[string]string {
	result := make(map[string]string)
	for _, tag := range rawTags {
		splittedTag := strings.Split(tag, ":")
		result[splittedTag[0]] = splittedTag[1]
	}
	return result
}
