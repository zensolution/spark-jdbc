package com.zensolution.jdbc.parquet.internal;

import scala.sys.Prop;

import java.util.Properties;

public class ConnectionInfo {
    private String url;
    private Properties prop = new Properties();

    public ConnectionInfo(String url, Properties info) {
        this.url = url;
        this.prop = info;
    }

}
