package com.zensolution.jdbc.parquet.internal;

import scala.sys.Prop;

import java.util.Locale;
import java.util.Properties;

public class ConnectionInfo {
    private String path;
    private SupportedFormat format;
    private Properties prop = new Properties();

    public ConnectionInfo(String path, Properties info) {
        this.path = path;
        this.prop = info;
        this.format = parseFormat(info.getProperty("format"));
    }

    private SupportedFormat parseFormat(String format) {
        return format==null? SupportedFormat.PARQUET : SupportedFormat.valueOf(format.toUpperCase(Locale.getDefault()));
    }

    public String getPath() {
        return path;
    }

    public Properties getProperties() {
        return prop;
    }

    public SupportedFormat getFormat() {
        return format;
    }
}
