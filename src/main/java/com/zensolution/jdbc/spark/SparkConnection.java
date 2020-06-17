package com.zensolution.jdbc.spark;

import com.zensolution.jdbc.spark.internal.ConnectionInfo;
import com.zensolution.jdbc.spark.internal.SparkService;
import com.zensolution.jdbc.spark.jdbc.AbstractJdbcConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SparkConnection extends AbstractJdbcConnection {

    private static final Logger logger = LoggerFactory.getLogger(SparkConnection.class);

    /**
     * Directory where the Parquet files to use are located
     */
    private ConnectionInfo connectionInfo;

    /**
     * Directory where the Parquet files to use are located
     */
    private Properties info;

    /**
     * Stores whether this Connection is closed or not
     */
    private boolean closed = false;

    /*
     *  Unified interface to access spark
     */
    private SparkService sparkService;

    /**
     * Collection of all created Statements
     */
    private List<Statement> statements = new ArrayList<Statement>();

    /**
     * Creates a new CsvConnection that takes the supplied path
     *
     * @param path directory where the Parquet files are located
     */
    protected SparkConnection(String path, Properties info) throws SQLException {
        // validate argument(s)
        if (path == null || path.length() == 0) {
            throw new IllegalArgumentException("Unknown Path");
        }
        this.connectionInfo = new ConnectionInfo(path, info);
        this.info = info;
        this.sparkService = new SparkService(connectionInfo);
        logger.info("SparkConnection - path="+path+" info-"+info);
    }


    public ConnectionInfo getConnectionInfo() {
        return connectionInfo;
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkOpen();
        SparkStatement statement = new SparkStatement(this, sparkService);
        logger.info("SparkConnection:createStatement()");
        statements.add(statement);
        return statement;
    }
    @Override
    public boolean getAutoCommit() throws SQLException {
        return false;
    }

    @Override
    public void close() throws SQLException {
        if ( !this.closed ) {
            sparkService.close();
            this.closed = true;
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return true;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return false;
    }

    @Override
    public String getSchema() throws SQLException {
        return "";
    }

    private void checkOpen() throws SQLException {
        if (closed) {
            throw new SQLException("Connection has been closed.");
        }
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        logger.info("SparkConnection-getMetaData()");
        return new SparkDatabaseMetaData(this, this.sparkService);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkOpen();
        return TRANSACTION_NONE;
    }

    protected String getURL() {
        return SparkDriver.URL_PREFIX + connectionInfo.getPath();
    }
}
