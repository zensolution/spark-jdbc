package com.zensolution.jdbc.spark;

import com.zensolution.jdbc.spark.jdbc.AbstractJdbcResultSetMetaData;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SparkResultSetMetaData extends AbstractJdbcResultSetMetaData {

    private static final Logger logger = LoggerFactory.getLogger(SparkResultSet.class);

    private List<StructField> structFields = new ArrayList<>();

    public SparkResultSetMetaData(StructType structType) {
        for (int i=0; i<structType.fields().length; i++) {
            this.structFields.add(structType.fields()[i]);
        }
    }

    public int indexOf(String column) throws SQLException {
        for ( int i=0; i<structFields.size(); i++ ) {
            if ( structFields.get(i).name().equalsIgnoreCase(column) ) {
                return i+1;
            }
        }
        throw new SQLException("Unknown column '" + column + "'");
    }

    @Override
    public int getColumnCount() throws SQLException {
        return structFields.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return this.structFields.get(column-1).nullable() ? columnNullable : columnNoNulls;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        StructField structField = this.structFields.get(column-1);
        if ( structField.dataType() instanceof DecimalType ) {
            return ((DecimalType)structField.dataType()).precision();
        } else {
            return this.structFields.get(column-1).dataType().defaultSize();
        }
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return this.structFields.get(column-1).name();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }


    @Override
    public int getScale(int column) throws SQLException {
        StructField structField = this.structFields.get(column-1);
        if ( structField.dataType() instanceof DecimalType ) {
            return ((DecimalType) structField.dataType()).scale();
        } else {
            return 0;
        }
    }

    public StructField getStructField(int column) {
        return this.structFields.get(column-1);
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return JdbcUtils.getCommonJDBCType(this.structFields.get(column-1).dataType()).get().jdbcNullType();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return this.structFields.get(column-1).dataType().typeName();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return JdbcUtils.getCommonJDBCType(this.structFields.get(column-1).dataType()).get().databaseTypeDefinition();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }
}
