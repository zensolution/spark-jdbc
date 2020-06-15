package com.zensolution.jdbc.spark.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public abstract class AbstractJdbcResultSetMetaData implements ResultSetMetaData {
    @Override
    public int getPrecision(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getTableName(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }
}
