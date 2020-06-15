package com.zensolution.jdbc.spark;

import com.zensolution.jdbc.spark.internal.SparkService;
import com.zensolution.jdbc.spark.internal.Versions;
import com.zensolution.jdbc.spark.jdbc.AbstractJdbcDatabaseMetaData;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SparkDatabaseMetaData extends AbstractJdbcDatabaseMetaData {

    private SparkService sparkService;
    private SparkConnection connection;

    public SparkDatabaseMetaData(SparkConnection connection, SparkService sparkService) {
        this.sparkService = sparkService;
        this.connection = connection;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        return this.connection.getURL();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return true;
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return Versions.Major + "." + Versions.Minor;
    }

    @Override
    public int getDriverMajorVersion() {
        return Versions.Major;
    }

    @Override
    public int getDriverMinorVersion() {
        return Versions.Minor;
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        return new SparkResultSet(sparkService.getTables());
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return new SparkResultSet(sparkService.getColumns(tableNamePattern));
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return Versions.Major;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return Versions.Minor;
    }
}
