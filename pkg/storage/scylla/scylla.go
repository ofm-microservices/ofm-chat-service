package scylla

import (
	"strings"

	"github.com/gocql/gocql"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"chat-service/config"
)

// ConnectAndEnsureSchema opens the Scylla session after applying the saga migrations.
func ConnectAndEnsureSchema(cfg config.ScyllaConfig, log logging.Logger) (*gocql.Session, error) {
	cluster := gocql.NewCluster(cfg.Hosts...)
	cluster.Port = cfg.Port
	cluster.Timeout = cfg.ConnectTimeout
	cluster.ConnectTimeout = cfg.ConnectTimeout
	cluster.MaxWaitSchemaAgreement = cfg.MaxWaitSchemaAgreement
	cluster.Consistency = parseConsistency(cfg.Consistency)
	if cfg.Username != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{Username: cfg.Username, Password: cfg.Password}
	}

	cluster.Keyspace = cfg.Keyspace
	return cluster.CreateSession()
}

func parseConsistency(level string) gocql.Consistency {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "one":
		return gocql.One
	case "localquorum":
		return gocql.LocalQuorum
	case "all":
		return gocql.All
	default:
		return gocql.Quorum
	}
}
