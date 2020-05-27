package com.zensolution.jdbc.parquet.internal;

import scala.sys.Prop;

import java.util.Properties;

public class ConnectionInfo {
    private String path;
    private Properties prop = new Properties();

    public ConnectionInfo(String path, Properties info) {
        this.path = path;
        this.prop = info;
    }

    public String getPath() {
        return path;
    }

    public Properties getProperties() {
        return prop;
    }
}
