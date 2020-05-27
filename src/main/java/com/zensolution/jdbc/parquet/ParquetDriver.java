package com.zensolution.jdbc.parquet;

import com.zensolution.jdbc.parquet.internal.Versions;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
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
        String urlProperties = url.substring(URL_PREFIX.length());
        int questionIndex = urlProperties.indexOf('?');
        String path = questionIndex >= 0 ? urlProperties.substring(0, questionIndex) : urlProperties;
        if (questionIndex >= 0) {
            Properties prop = parseUrlIno(urlProperties.substring(questionIndex+1));
            info.putAll(prop);
        }
        LOGGER.log(Level.FINE, "ParquetDriver:connect() - filePath=" + path);
        return new ParquetConnection(path, info);
    }

    private Properties parseUrlIno(String urlProperties) throws SQLException {
        Properties info = new Properties();
        String[] split = urlProperties.split("&");
        for (int i = 0; i < split.length; i++) {
            String[] property = split[i].split("=");
            try {
                if (property.length == 2) {
                    String key = URLDecoder.decode(property[0], "UTF-8");
                    String value = URLDecoder.decode(property[1], "UTF-8");
                    info.setProperty(key, value);
                } else {
                    throw new SQLException("invalid Property: " + split[i]);
                }
            } catch (UnsupportedEncodingException e) {
                // we know UTF-8 is available
            }
        }
        return info;
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
