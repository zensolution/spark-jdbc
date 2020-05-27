package com.zensolution.jdbc.parquet;

import com.zensolution.jdbc.parquet.internal.Versions;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ParquetDriver implements Driver {

    private static final Logger LOGGER = Logger.getLogger("com.zensolution.jdbc.parquet.ParquetDriver");

    public final static String URL_PREFIX = "com.zensolution.jdbc.parquet:";

    static {
        try {
            register();
        } catch (SQLException e) {
            LOGGER.log(Level.WARNING, "Unable to register com.zensolution.jdbc.parquet JDBC driver", e);
        }
    }

    public static synchronized void register() throws SQLException {
        DriverManager.registerDriver(new ParquetDriver());
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {

        if (info == null) {
            info = new Properties();
        }
        if (!acceptsURL(url)) {
            return null;
        }

        // get filepath from url
        String filePath = url.substring(URL_PREFIX.length());
        LOGGER.log(Level.FINE, "ParquetDriver:connect() - filePath=" + filePath);
        return new ParquetConnection(filePath, info);
    }

    private Properties parseUrlIno(String propertyStr) {
        //Arrays.stream(propertyStr.split("&")).map();
        return new Properties();
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        LOGGER.log(Level.FINE, "ParquetDriver:accept() - url=" + url);
        return url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return Versions.Major;
    }

    @Override
    public int getMinorVersion() {
        return Versions.Minor;
    }

    @Override
    public boolean jdbcCompliant() {
        // This has to be false since we are not fully SQL-92 compliant
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return Logger.getLogger(getClass().getPackage().getName());
    }

}
