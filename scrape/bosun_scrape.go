package scrape

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	mq "code.byted.org/inf/metrics-query"
	"github.com/pkg/errors"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

// https://doc.bytedance.net/docs/2080/2717/29482/

var clusterOptionCN *mq.ClusterOption

func init() {
	// 集群（鉴权）配置，这里直接用预设好的配置。
	clusterConfig := mq.DefaultClusterConfig.Proxy
	// 集群配置也是一个 map ，键是集群字符串，值是鉴权选项。
	clusterOptionCN = clusterConfig.Get("cn")
	if clusterOptionCN != nil {
		// 注意：必须设置注册好的查询账户，因为公共账号已经从流量上封死，总是返回 429 状态码。
		clusterOptionCN.SetTenant("ad.oe.metrics", "bd00e4ed314f4fd5ad78865ad53fa290")
		_, err := clusterOptionCN.RefreshToken(true, 10*time.Second)
		if err != nil {
			log.Println("RefreshToken failed, err: ", err)
		}
		go func() {
			for range time.Tick(time.Second * 1800) {
				_, err := clusterOptionCN.RefreshToken(false, 10*time.Second)
				if err != nil {
					log.Println("RefreshToken failed, err: ", err)
				}
			}
		}()
	}
}

// bosunScraper implements the scraper interface for a target.
type bosunScraper struct {
	*Target

	client  *http.Client
	req     *http.Request
	timeout time.Duration

	gzipr *gzip.Reader
	buf   *bufio.Reader

	// added for bosun
	rule        string
	metricsName string
}

func (s *bosunScraper) scrape(ctx context.Context, w io.Writer) (string, error) {

	req, err := http.NewRequest("POST",
		"http://metrics.byted.org/proxy/bosun/api/expr?_region=cn", bytes.NewBufferString(s.rule))
	if err != nil {
		return "", err
	}
	req.Header.Add("Accept", acceptHeader)
	req.Header.Set("User-Agent", userAgentHeader)
	req.Header.Add("Accept-Encoding", "gzip")
	req.Header.Set("X-Prometheus-Scrape-Timeout-Seconds", fmt.Sprintf("%f", s.timeout.Seconds()))
	req.Header.Set("Authorization", clusterOptionCN.AccessToken())
	s.req = req

	resp, err := s.client.Do(s.req.WithContext(ctx))

	if err != nil {
		return "", err
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		log.Println("request failed", s.req.Header.Get("Authorization"), "status", resp.Status)
		return "", errors.Errorf("server returned HTTP status %s", resp.Status)
	}

	if resp.Header.Get("Content-Encoding") != "gzip" {
		err = s.convertCopy(w, resp.Body)
		if err != nil {
			return "", err
		}
		return resp.Header.Get("Content-Type"), nil
	}

	if s.gzipr == nil {
		s.buf = bufio.NewReader(resp.Body)
		s.gzipr, err = gzip.NewReader(s.buf)
		if err != nil {
			return "", err
		}
	} else {
		s.buf.Reset(resp.Body)
		if err = s.gzipr.Reset(s.buf); err != nil {
			return "", err
		}
	}

	err = s.convertCopy(w, s.gzipr)
	s.gzipr.Close()
	if err != nil {
		return "", err
	}
	return resp.Header.Get("Content-Type"), nil
}

// 重写返回为 prometheus 风格

func (s *bosunScraper) convertCopy(writer io.Writer, reader io.Reader) error {
	b := &BosunResponse{}
	d := json.NewDecoder(reader)
	if err := d.Decode(b); err != nil {
		return err
	}
	t := dto.MetricType_UNTYPED
	m := &dto.MetricFamily{
		Name:   &s.metricsName,
		Type:   &t,
		Metric: convertBosunResponse2Metrics(b),
	}
	log.Println("call convertCopy: ", debugString(b), " ---> ", debugString(m))
	_, err := expfmt.MetricFamilyToText(writer, m)
	return err
}

func debugString(v interface{}) string {
	data, _ := json.Marshal(v)
	return string(data)
}

func convertBosunResponse2Metrics(b *BosunResponse) (ret []*dto.Metric) {
	for _, r := range b.Results {
		if r.Value.Number != nil {
			ret = append(ret, &dto.Metric{
				Label: convertTagSet2Labels(r.Group),
				Untyped: &dto.Untyped{
					Value: r.Value.Number,
				},
			})
		} else if r.Value.Series != nil {
			for k, v := range *r.Value.Series {
				tk, tv := k, v
				ret = append(ret, &dto.Metric{
					Label: convertTagSet2Labels(r.Group),
					Untyped: &dto.Untyped{
						Value: &tv,
					},
					TimestampMs: &tk,
				})
			}
		}
	}
	return
}

func convertTagSet2Labels(set TagSet) (ret []*dto.LabelPair) {
	for k, v := range set {
		tk, tv := k, v
		ret = append(ret, &dto.LabelPair{
			Name:  &tk,
			Value: &tv,
		})
	}
	return
}
