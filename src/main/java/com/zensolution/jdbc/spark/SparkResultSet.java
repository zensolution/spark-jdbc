package com.zensolution.jdbc.spark;

import com.zensolution.jdbc.spark.internal.ConnectionInfo;
import com.zensolution.jdbc.spark.internal.SparkService;
import com.zensolution.jdbc.spark.jdbc.AbstractJdbcResultSet;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.types.ShortType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.Charset;

import java.sql.Date;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Iterator;

public class SparkResultSet extends AbstractJdbcResultSet {

    private static final Logger logger = LoggerFactory.getLogger(SparkResultSet.class);

    /**
     * Result of last call to next()
     */
    private boolean nextResult = true;

    /**
     * Stores whether this Resultset is closed or not
     */
    private boolean isClosed = false;

    /**
     * Metadata for this ResultSet
     */
    private SparkResultSetMetaData resultSetMetaData;

    /**
     * True if the last obtained column value was SQL NULL as specified by {@link #wasNull}. The value
     * is always updated by the method.
     */
    protected boolean wasNullFlag = false;

    private Iterator<Row> dataListIterator;
    private Row current;
    private int count = 0;

    protected SparkResultSet(ConnectionInfo connectionInfo, String sqlText, SparkService sparkService) throws SQLException, ParseException {
        logger.info("SparkResultSet - sqlText:" + sqlText);
        Dataset<Row> ds = sparkService.executeQuery(sqlText);
        resultSetMetaData = new SparkResultSetMetaData(ds.schema());
        count = (int)ds.count();
        this.dataListIterator = ds.toLocalIterator();
    }

    protected SparkResultSet(Dataset<Row> ds) {
        resultSetMetaData = new SparkResultSetMetaData(ds.schema());
        count = (int)ds.count();
        this.dataListIterator = ds.toLocalIterator();
    }

    public int getCount() {
        return count;
    }

    @Override
    public boolean next() throws SQLException {
        checkOpen();
        if ( this.dataListIterator.hasNext() ) {
            this.current = this.dataListIterator.next();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void close() throws SQLException {
        this.isClosed = true;
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkOpen();
        return wasNullFlag;
    }

    private void checkNull(int columnIndex) {
        wasNullFlag = current.isNullAt(columnIndex-1);
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        checkOpen();
        checkNull(columnIndex);
        return current.getString(columnIndex-1);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        checkOpen();
        checkNull(columnIndex);
        return current.getBoolean(columnIndex-1);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        checkOpen();
        checkNull(columnIndex);
        return current.getByte(columnIndex-1);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        checkOpen();
        checkNull(columnIndex);
        return current.getShort(columnIndex-1);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        checkOpen();
        checkNull(columnIndex);

        if ( current.isNullAt(columnIndex-1) ) {
            return 0;
        } else if ( resultSetMetaData.getStructField(columnIndex).dataType() instanceof ShortType) {
            return new Short(current.getShort(columnIndex-1)).intValue();
        } else {
            return current.getInt(columnIndex-1);
        }
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        checkOpen();
        checkNull(columnIndex);
        return current.isNullAt(columnIndex-1) ? 0 : current.getLong(columnIndex-1);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        checkOpen();
        checkNull(columnIndex);
        return current.isNullAt(columnIndex-1) ? 0 : current.getFloat(columnIndex-1);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        checkOpen();
        checkNull(columnIndex);
        return current.isNullAt(columnIndex-1) ? 0 : current.getDouble(columnIndex-1);
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        checkOpen();
        checkNull(columnIndex);
        return current.getDecimal(columnIndex-1).setScale(scale);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        checkOpen();
        checkNull(columnIndex);
        String s = getString(columnIndex);
        if (s != null) {
            return s.getBytes(Charset.defaultCharset());
        }
        return null;
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        checkOpen();
        checkNull(columnIndex);
        return current.getDate(columnIndex-1);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        checkOpen();
        checkNull(columnIndex);
        return current.getTimestamp(columnIndex-1);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        checkNull(columnIndex);
        String s = getString(columnIndex);
        if (s != null) {
            return parseAsciiStream(s);
        }
        return null;
    }

    private InputStream parseAsciiStream(String str) {
        return (str == null) ? null : new ByteArrayInputStream(str.getBytes());
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return getAsciiStream(columnIndex);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return getAsciiStream(columnIndex);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        checkOpen();
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        checkOpen();
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        checkOpen();
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        checkOpen();
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        checkOpen();
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        checkOpen();
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        checkOpen();
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        checkOpen();
        return getDouble(findColumn(columnLabel));
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        checkOpen();
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        checkOpen();
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        checkOpen();
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        checkOpen();
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        checkOpen();
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(findColumn(columnLabel));
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(findColumn(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return this.resultSetMetaData;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        checkOpen();
        checkNull(columnIndex);
        return current.get(columnIndex-1);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return this.resultSetMetaData.indexOf(columnLabel);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        checkOpen();
        checkNull(columnIndex);
        return current.getDecimal(columnIndex-1);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        checkOpen();
        return getBigDecimal(findColumn(columnLabel));
    }

    private void checkOpen() throws SQLException {
        if (isClosed) {
            throw new SQLException("closedResultSet");
        }
    }
}
