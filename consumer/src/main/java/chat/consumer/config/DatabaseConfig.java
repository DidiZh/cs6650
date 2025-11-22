package chat.consumer.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Database configuration using HikariCP connection pool
 */
public class DatabaseConfig {
    private static final Logger log = LoggerFactory.getLogger(DatabaseConfig.class);

    private final String url;
    private final String username;
    private final String password;
    private final String driver;

    // Connection pool settings
    private final int maximumPoolSize;
    private final int minimumIdle;
    private final long connectionTimeout;
    private final long idleTimeout;
    private final long maxLifetime;

    private HikariDataSource dataSource;

    public DatabaseConfig() throws IOException {
        Properties props = new Properties();

        // Load from classpath
        try (InputStream in = getClass().getClassLoader()
                .getResourceAsStream("database.properties")) {
            if (in == null) {
                throw new IOException("database.properties not found in classpath");
            }
            props.load(in);
        }

        this.url = props.getProperty("db.url");
        this.username = props.getProperty("db.username");
        this.password = props.getProperty("db.password");
        this.driver = props.getProperty("db.driver");

        this.maximumPoolSize = Integer.parseInt(props.getProperty("db.pool.maximumPoolSize", "20"));
        this.minimumIdle = Integer.parseInt(props.getProperty("db.pool.minimumIdle", "5"));
        this.connectionTimeout = Long.parseLong(props.getProperty("db.pool.connectionTimeout", "30000"));
        this.idleTimeout = Long.parseLong(props.getProperty("db.pool.idleTimeout", "600000"));
        this.maxLifetime = Long.parseLong(props.getProperty("db.pool.maxLifetime", "1800000"));
    }

    /**
     * Initialize HikariCP DataSource
     */
    public void initialize() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        config.setDriverClassName(driver);

        // Pool settings
        config.setMaximumPoolSize(maximumPoolSize);
        config.setMinimumIdle(minimumIdle);
        config.setConnectionTimeout(connectionTimeout);
        config.setIdleTimeout(idleTimeout);
        config.setMaxLifetime(maxLifetime);

        // Performance settings
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.addDataSourceProperty("useServerPrepStmts", "true");

        this.dataSource = new HikariDataSource(config);

        log.info("Database connection pool initialized: url={}, poolSize={}, minIdle={}",
                url, maximumPoolSize, minimumIdle);
    }

    /**
     * Get DataSource for database operations
     */
    public DataSource getDataSource() {
        if (dataSource == null) {
            throw new IllegalStateException("DataSource not initialized. Call initialize() first.");
        }
        return dataSource;
    }

    /**
     * Close the connection pool
     */
    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            log.info("Database connection pool closed");
        }
    }
}