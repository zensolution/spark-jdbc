package com.zensolution.jdbc.spark.internal;

import java.io.File;

public enum SupportedFormat {
    PARQUET, CSV, DELTA;

    public String getSparkPath(String path, String table) {
        switch (this) {
            case PARQUET:
            case CSV:
                return new File(path, table).getAbsolutePath();
            case DELTA:
                return path.endsWith("/") ? path + table : path + "/" + table;
            default:
                throw new RuntimeException("Unknown path " + path);
        }
    }
}
