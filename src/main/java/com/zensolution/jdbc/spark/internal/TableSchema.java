package com.zensolution.jdbc.spark.internal;

import java.util.Objects;

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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableSchema that = (TableSchema) o;
        return Objects.equals(path, that.path) &&
                Objects.equals(table, that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, table);
    }
}
