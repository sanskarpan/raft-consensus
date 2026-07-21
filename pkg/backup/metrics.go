package backup

import "github.com/prometheus/client_golang/prometheus"

var (
	uploadDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "raft_backup_upload_duration_seconds",
		Help:    "Duration of backup upload operations.",
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 10),
	})
	uploadBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "raft_backup_upload_bytes_total",
		Help: "Total bytes uploaded to object storage.",
	})
	uploadErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "raft_backup_upload_errors_total",
		Help: "Total backup upload failures.",
	})
	downloadDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "raft_backup_download_duration_seconds",
		Help:    "Duration of backup download/restore operations.",
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 10),
	})
	restoreErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "raft_backup_restore_errors_total",
		Help: "Total backup restore failures.",
	})
)

func mustRegister(c prometheus.Collector) {
	if err := prometheus.DefaultRegisterer.Register(c); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
			panic(err)
		}
	}
}

func init() {
	mustRegister(uploadDuration)
	mustRegister(uploadBytes)
	mustRegister(uploadErrors)
	mustRegister(downloadDuration)
	mustRegister(restoreErrors)
}
