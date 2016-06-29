package exporter

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/prometheus/client_golang/prometheus"
)

// RedisHost represents a set of Redis Hosts to health check.
type RedisHost struct {
	Addrs     []string
	Passwords []string
}

// Exporter implementes the prometheus.Exporter interface, and exports Redis metrics.
type Exporter struct {
	redis        RedisHost
	namespace    string
	duration     prometheus.Gauge
	scrapeErrors prometheus.Gauge
	totalScrapes prometheus.Counter
	metrics      map[string]*prometheus.GaugeVec
	sync.RWMutex
}

type scrapeResult struct {
	Name  string
	Value float64
	Addr  string
	DB    string
}

func (e *Exporter) initGauges() {

	e.metrics = map[string]*prometheus.GaugeVec{}
	e.metrics["db_keys_total"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace,
		Name:      "db_keys_total",
		Help:      "Total number of keys by DB",
	}, []string{"addr", "db"})
	e.metrics["db_expiring_keys_total"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace,
		Name:      "db_expiring_keys_total",
		Help:      "Total number of expiring keys by DB",
	}, []string{"addr", "db"})
	e.metrics["db_avg_ttl_seconds"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace,
		Name:      "db_avg_ttl_seconds",
		Help:      "Avg TTL in seconds",
	}, []string{"addr", "db"})
}

// NewRedisExporter returns a new exporter of Redis metrics.
func NewRedisExporter(redis RedisHost, namespace string) *Exporter {
	e := Exporter{
		redis:     redis,
		namespace: namespace,

		duration: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "exporter_last_scrape_duration_seconds",
			Help:      "The last scrape duration.",
		}),
		totalScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "exporter_scrapes_total",
			Help:      "Current total redis scrapes.",
		}),
		scrapeErrors: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "exporter_last_scrape_error",
			Help:      "The last scrape error status.",
		}),
	}

	e.initGauges()
	return &e
}

// Describe outputs Redis metric descriptions.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {

	for _, m := range e.metrics {
		m.Describe(ch)
	}
	ch <- e.duration.Desc()
	ch <- e.totalScrapes.Desc()
	ch <- e.scrapeErrors.Desc()
}

// Collect fetches new metrics from the RedisHost and updates the appropriate metrics.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	scrapes := make(chan scrapeResult)

	e.Lock()
	defer e.Unlock()

	e.initGauges()
	go e.scrape(scrapes)
	e.setMetrics(scrapes)

	ch <- e.duration
	ch <- e.totalScrapes
	ch <- e.scrapeErrors
	e.collectMetrics(ch)
}

func includeMetric(name string) bool {

	incl := map[string]bool{
		"uptime_in_seconds":       true,
		"connected_clients":       true,
		"blocked_clients":         true,
		"used_memory":             true,
		"used_memory_rss":         true,
		"used_memory_peak":        true,
		"used_memory_lua":         true,
		"mem_fragmentation_ratio": true,

		"total_connections_received": true,
		"total_commands_processed":   true,
		"instantaneous_ops_per_sec":  true,
		"total_net_input_bytes":      true,
		"total_net_output_bytes":     true,
		"rejected_connections":       true,

		"expired_keys":    true,
		"evicted_keys":    true,
		"keyspace_hits":   true,
		"keyspace_misses": true,
		"pubsub_channels": true,
		"pubsub_patterns": true,

		"connected_slaves": true,

		"used_cpu_sys":           true,
		"used_cpu_user":          true,
		"used_cpu_sys_children":  true,
		"used_cpu_user_children": true,

		"loading":                     true,
		"rdb_changes_since_last_save": true,
		"rdb_bgsave_in_progress":      true,
		"rdb_last_save_time":          true,
		"rdb_last_bgsave_status":      true,
		"rdb_last_bgsave_time_sec":    true,
		"aof_enabled":                 true,
		"aof_rewrite_in_progress":     true,
		"aof_rewrite_scheduled":       true,
		"aof_last_bgrewrite_status":   true,
		"aof_last_write_status":       true,

		"master_repl_offset":             true,
		"repl_backlog_active":            true,
		"repl_backlog_size":              true,
		"repl_backlog_first_byte_offset": true,
		"repl_backlog_histlen":           true,

		"cluster_enabled": true,
	}

	if strings.HasPrefix(name, "db") {
		return true
	}

	_, ok := incl[name]

	return ok
}

func includeClusterInfoMetric(name string) bool {

	incl := map[string]bool{
		"cluster_state":                   true,
		"cluster_slots_assigned":          true,
		"cluster_slots_ok":                true,
		"cluster_slots_pfail":             true,
		"cluster_slots_fail":              true,
		"cluster_known_nodes":             true,
		"cluster_size":                    true,
		"cluster_current_epoch":           true,
		"cluster_my_epoch":                true,
		"cluster_stats_messages_sent":     true,
		"cluster_stats_messages_received": true,
	}

	_, ok := incl[name]

	return ok
}

func extractInfoMetrics(info, addr string, scrapes chan<- scrapeResult) error {

	lines := strings.Split(info, "\r\n")

	for _, line := range lines {

		if (len(line) < 2) || line[0] == '#' || (!strings.Contains(line, ":")) {
			continue
		}
		split := strings.Split(line, ":")
		if len(split) != 2 || !includeMetric(split[0]) {
			continue
		}
		// status: ok or fail
		if strings.HasPrefix(split[1], "ok") {
			scrapes <- scrapeResult{Name: split[0], Addr: addr, Value: 1}
			continue
		}
		if strings.HasPrefix(split[1], "fail") {
			scrapes <- scrapeResult{Name: split[0], Addr: addr, Value: 0}
			continue
		}

		if strings.HasPrefix(split[0], "db") {
			// example: db0:keys=1,expires=0,avg_ttl=0

			db := split[0]
			stats := split[1]
			split := strings.Split(stats, ",")
			if len(split) != 3 && len(split) != 2 {
				log.Printf("unexpected db stats format: %s", stats)
				continue
			}

			extract := func(s string) (val float64) {
				split := strings.Split(s, "=")
				if len(split) != 2 {
					log.Printf("unexpected db stats format: %s", s)
					return 0
				}
				val, err := strconv.ParseFloat(split[1], 64)
				if err != nil {
					log.Printf("couldn't parse %s, err: %s", split[1], err)
				}
				return
			}

			scrapes <- scrapeResult{Name: "db_keys_total", Addr: addr, DB: db, Value: extract(split[0])}
			scrapes <- scrapeResult{Name: "db_expiring_keys_total", Addr: addr, DB: db, Value: extract(split[1])}
			if len(split) > 2 {
				scrapes <- scrapeResult{Name: "db_avg_ttl_seconds", Addr: addr, DB: db, Value: (extract(split[2]) / 1000)}
			}

			continue
		}

		val, err := strconv.ParseFloat(split[1], 64)
		if err != nil {
			log.Printf("couldn't parse %s, err: %s", split[1], err)
			continue
		}
		scrapes <- scrapeResult{Name: split[0], Addr: addr, Value: val}
	}
	return nil
}

//cluster info
func extractClusterInfoMetrics(clusterinfo, addr string, scrapes chan<- scrapeResult) error {

	lines := strings.Split(clusterinfo, "\r\n")

	for _, line := range lines {

		if (len(line) < 2) || line[0] == '#' || (!strings.Contains(line, ":")) {
			continue
		}
		split := strings.Split(line, ":")
		if len(split) != 2 || !includeClusterInfoMetric(split[0]) {
			continue
		}

		// status: ok or fail
		if strings.HasPrefix(split[1], "ok") {
			scrapes <- scrapeResult{Name: split[0], Addr: addr, Value: 1}
			continue
		}
		if strings.HasPrefix(split[1], "fail") {
			scrapes <- scrapeResult{Name: split[0], Addr: addr, Value: 0}
			continue
		}

		val, err := strconv.ParseFloat(split[1], 64)
		if err != nil {
			log.Printf("couldn't parse %s, err: %s", split[1], err)
			continue
		}
		scrapes <- scrapeResult{Name: split[0], Addr: addr, Value: val}

	}
	return nil
}

func extractClusterNodesMetrics(nodes, addr string, scrapes chan<- scrapeResult) error {

	lines := strings.Split(nodes, "\r\n")

	for _, line := range lines {
		split := strings.Split(line, "\n")
		rows := strings.Count(line, "\n")
		for pos := 0; pos < rows; pos++ {
			scrapes <- scrapeResult{Name: fmt.Sprintf("cluster_nodes_stats"), Addr: fmt.Sprint(split[pos]), Value: 1}
		}
		continue

	}
	return nil
}

func extractConfigMetrics(config []string, addr string, scrapes chan<- scrapeResult) error {

	if len(config)%2 != 0 {
		return fmt.Errorf("invalid config: %#v", config)
	}

	for pos := 0; pos < len(config)/2; pos++ {
		val, err := strconv.ParseFloat(config[pos*2+1], 64)
		if err != nil {
			log.Printf("couldn't parse %s, err: %s", config[pos*2+1], err)
			continue
		}
		scrapes <- scrapeResult{Name: fmt.Sprintf("config_%s", config[pos*2]), Addr: addr, Value: val}
	}
	return nil
}

func extractMemoryPolicyMetrics(maxmemorypolicy []string, addr string, scrapes chan<- scrapeResult) error {

	if len(maxmemorypolicy)%2 != 0 {
		return fmt.Errorf("invalid config: %#v", maxmemorypolicy)
	}

	for pos := 0; pos < len(maxmemorypolicy)/2; pos++ {
		if strings.HasPrefix(maxmemorypolicy[pos*2+1], "volatile-lru") {
			scrapes <- scrapeResult{Name: fmt.Sprintf("config_%s", strings.Replace(maxmemorypolicy[pos*2], "-", "_", 1)), Addr: addr, Value: 1}
			continue
		}
		if strings.HasPrefix(maxmemorypolicy[pos*2+1], "allkeys-lru") {
			scrapes <- scrapeResult{Name: fmt.Sprintf("config_%s", strings.Replace(maxmemorypolicy[pos*2], "-", "_", 1)), Addr: addr, Value: 2}
			continue
		}
		if strings.HasPrefix(maxmemorypolicy[pos*2+1], "volatile-random") {
			scrapes <- scrapeResult{Name: fmt.Sprintf("config_%s", strings.Replace(maxmemorypolicy[pos*2], "-", "_", 1)), Addr: addr, Value: 3}
			continue
		}
		if strings.HasPrefix(maxmemorypolicy[pos*2+1], "volatile-ttl") {
			scrapes <- scrapeResult{Name: fmt.Sprintf("config_%s", strings.Replace(maxmemorypolicy[pos*2], "-", "_", 1)), Addr: addr, Value: 4}
			continue
		}
		if strings.HasPrefix(maxmemorypolicy[pos*2+1], "noeviction") {
			scrapes <- scrapeResult{Name: fmt.Sprintf("config_%s", strings.Replace(maxmemorypolicy[pos*2], "-", "_", 1)), Addr: addr, Value: 5}
			continue
		}

	}
	return nil
}

func (e *Exporter) scrape(scrapes chan<- scrapeResult) {

	defer close(scrapes)
	now := time.Now().UnixNano()
	e.totalScrapes.Inc()

	errorCount := 0
	for idx, addr := range e.redis.Addrs {
		c, err := redis.Dial("tcp", addr)
		if err != nil {
			log.Printf("redis err: %s", err)
			errorCount++
			continue
		}
		if len(e.redis.Passwords) > idx && e.redis.Passwords[idx] != "" {
			if _, err := c.Do("AUTH", e.redis.Passwords[idx]); err != nil {
				log.Printf("redis err: %s", err)
				errorCount++
				continue
			}
		}
		//info
		info, err := redis.String(c.Do("INFO"))
		if err == nil {
			err = extractInfoMetrics(info, addr, scrapes)
		}
		if err != nil {
			log.Printf("redis err: %s", err)
			errorCount++
		}
		//cluster info
		clusterinfo, err := redis.String(c.Do("CLUSTER", "INFO"))
		if err == nil {
			err = extractClusterInfoMetrics(clusterinfo, addr, scrapes)
		}
		if err != nil {
			log.Printf("redis err: %s", err)
			errorCount++
		}
		//cluster nodes
		nodes, err := redis.String(c.Do("CLUSTER", "NODES"))
		if err == nil {
			err = extractClusterNodesMetrics(nodes, addr, scrapes)
		}
		if err != nil {
			log.Printf("redis err: %s", err)
			errorCount++
		}
		//config
		config, err := redis.Strings(c.Do("CONFIG", "GET", "maxmemory"))
		if err == nil {
			err = extractConfigMetrics(config, addr, scrapes)
		}
		if err != nil {
			log.Printf("redis err: %s", err)
			errorCount++
		}
		//config get maxmemory-policy
		maxmemorypolicy, err := redis.Strings(c.Do("CONFIG", "GET", "maxmemory-policy"))
		if err == nil {
			err = extractMemoryPolicyMetrics(maxmemorypolicy, addr, scrapes)
		}
		if err != nil {
			log.Printf("redis err: %s", err)
			errorCount++
		}

		c.Close()
	}

	e.scrapeErrors.Set(float64(errorCount))
	e.duration.Set(float64(time.Now().UnixNano()-now) / 1000000000)
}

func (e *Exporter) setMetrics(scrapes <-chan scrapeResult) {

	for scr := range scrapes {
		name := scr.Name
		if _, ok := e.metrics[name]; !ok {
			e.metrics[name] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Namespace: e.namespace,
				Name:      name,
			}, []string{"addr"})
		}
		var labels prometheus.Labels = map[string]string{"addr": scr.Addr}
		if len(scr.DB) > 0 {
			labels["db"] = scr.DB
		}
		e.metrics[name].With(labels).Set(float64(scr.Value))
	}
}

func (e *Exporter) collectMetrics(metrics chan<- prometheus.Metric) {
	for _, m := range e.metrics {
		m.Collect(metrics)
	}
}
