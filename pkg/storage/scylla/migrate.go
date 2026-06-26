package scylla

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/gocql/gocql"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"chat-service/config"
)

// RunMigrations applies the Scylla CQL migrations used by chat-service.
func RunMigrations(cfg config.ScyllaConfig, log logging.Logger) error {
	if log == nil {
		return ErrNilLogger
	}
	cluster := gocql.NewCluster(cfg.Hosts...)
	cluster.Port = cfg.Port
	cluster.Timeout = cfg.ConnectTimeout
	cluster.ConnectTimeout = cfg.ConnectTimeout
	cluster.MaxWaitSchemaAgreement = cfg.MaxWaitSchemaAgreement
	cluster.Consistency = parseConsistency(cfg.Consistency)
	if cfg.Username != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{Username: cfg.Username, Password: cfg.Password}
	}

	sys, err := cluster.CreateSession()
	if err != nil {
		return WrapCreateClusterSessionError(err)
	}
	defer sys.Close()

	if err := sys.Query(fmt.Sprintf(`
		CREATE KEYSPACE IF NOT EXISTS %s
		WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
	`, cfg.Keyspace)).Exec(); err != nil {
		return WrapEnsureSchemaError(err)
	}

	cluster.Keyspace = cfg.Keyspace
	app, err := cluster.CreateSession()
	if err != nil {
		return WrapCreateClusterSessionError(err)
	}
	defer app.Close()

	if err := ensureMigrationsTable(app, cfg.MigrationsTable); err != nil {
		return WrapEnsureSchemaError(err)
	}

	migrations, err := loadMigrations(cfg.MigrationsPath)
	if err != nil {
		return err
	}

	applied, err := loadAppliedMigrations(app, cfg.MigrationsTable)
	if err != nil {
		return err
	}

	for _, migration := range migrations {
		if _, ok := applied[migration.version]; ok {
			continue
		}
		if err := applyMigration(app, migration); err != nil {
			return err
		}
		if err := recordMigration(app, cfg.MigrationsTable, migration.version, migration.filename); err != nil {
			return err
		}
	}

	log.Info("scylla migrations applied",
		logging.String("keyspace", cfg.Keyspace),
		logging.String("migrations_path", cfg.MigrationsPath),
	)
	return nil
}

type migrationFile struct {
	version  int
	filename string
	queries  []string
}

func loadMigrations(path string) ([]migrationFile, error) {
	dir := path
	if strings.HasPrefix(dir, "file://") {
		dir = strings.TrimPrefix(dir, "file://")
	}
	if !filepath.IsAbs(dir) {
		abs, err := filepath.Abs(dir)
		if err != nil {
			return nil, WrapResolveMigrationsPathError(err)
		}
		dir = abs
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, WrapResolveMigrationsPathError(err)
	}
	var files []migrationFile
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".up.cql") {
			continue
		}
		version, err := parseMigrationVersion(entry.Name())
		if err != nil {
			return nil, err
		}
		raw, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, WrapRunMigrationsError(err)
		}
		files = append(files, migrationFile{
			version:  version,
			filename: entry.Name(),
			queries:  splitCQLStatements(string(raw)),
		})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].version < files[j].version })
	return files, nil
}

func parseMigrationVersion(filename string) (int, error) {
	parts := strings.SplitN(filename, "_", 2)
	if len(parts) == 0 {
		return 0, WrapRunMigrationsError(errors.New("invalid migration filename"))
	}
	version, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, WrapRunMigrationsError(err)
	}
	return version, nil
}

func splitCQLStatements(raw string) []string {
	lines := strings.Split(raw, "\n")
	var builder strings.Builder
	var out []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}
		builder.WriteString(line)
		builder.WriteString("\n")
	}
	for _, stmt := range strings.Split(builder.String(), ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt != "" {
			out = append(out, stmt)
		}
	}
	return out
}

func ensureMigrationsTable(app *gocql.Session, table string) error {
	return app.Query(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			version INT PRIMARY KEY,
			filename TEXT,
			applied_at TIMESTAMP
		)
	`, table)).Exec()
}

func loadAppliedMigrations(app *gocql.Session, table string) (map[int]struct{}, error) {
	iter := app.Query(fmt.Sprintf(`SELECT version FROM %s`, table)).Iter()
	defer iter.Close()
	applied := map[int]struct{}{}
	var version int
	for iter.Scan(&version) {
		applied[version] = struct{}{}
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	return applied, nil
}

func applyMigration(app *gocql.Session, migration migrationFile) error {
	for _, stmt := range migration.queries {
		if err := app.Query(stmt).Exec(); err != nil {
			return WrapRunMigrationsError(err)
		}
	}
	return nil
}

func recordMigration(app *gocql.Session, table string, version int, filename string) error {
	return app.Query(fmt.Sprintf(`INSERT INTO %s (version, filename, applied_at) VALUES (?, ?, toTimestamp(now()))`, table), version, filename).Exec()
}
