package com.zensolution.jdbc.spark.internal;

public class TableSchema {
    private String path;
    private String table;

    public TableSchema(String path, String table) {
        this.path = path;
        this.table = table;
    }

    public String getPath() {
        return path;
    }

    public String getTable() {
        return table;
    }

    @Override
    public int hashCode() {
        return String.format("%s-%s", path, table).hashCode();
    }
}
