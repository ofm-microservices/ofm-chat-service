package scylla

import (
	"context"
	"strings"

	"chat-service/config"
	"github.com/gocql/gocql"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/observability/cql"
	"github.com/ofm-microservices/ofm-common/pkg/resilience"
)

// ConnectAndEnsureSchema opens the Scylla session after applying the saga migrations.
func ConnectAndEnsureSchema(cfg config.ScyllaConfig, log logging.Logger) (*gocql.Session, error) {
	cluster := gocql.NewCluster(cfg.Hosts...)
	cluster.QueryObserver = cql.Observer{Service: "chat-service"}
	cluster.Port = cfg.Port
	cluster.Timeout = cfg.ConnectTimeout
	cluster.ConnectTimeout = cfg.ConnectTimeout
	cluster.MaxWaitSchemaAgreement = cfg.MaxWaitSchemaAgreement
	cluster.Consistency = parseConsistency(cfg.Consistency)
	if cfg.Username != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{Username: cfg.Username, Password: cfg.Password}
	}

	cluster.Keyspace = cfg.Keyspace
	var session *gocql.Session
	err := resilience.Retry(context.Background(), resilience.RetryPolicyFromEnv(), func(context.Context, int) error {
		var err error
		session, err = cluster.CreateSession()
		return err
	})
	return session, err
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
