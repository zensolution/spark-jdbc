package com.zensolution.jdbc.spark;

import com.zensolution.jdbc.spark.internal.SupportedFormat;
import junit.framework.Assert;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class SparkConnectionTest {

    @Test
    public void testConnectionInfo() throws Exception {
        Class.forName("com.zensolution.jdbc.spark.SparkDriver");
        SparkConnection conn = (SparkConnection)DriverManager
                .getConnection("com.zensolution.jdbc.spark:/Users/lubx/temp/console");
        Assert.assertEquals("/Users/lubx/temp/console", conn.getConnectionInfo().getPath());
        Assert.assertTrue(conn.getConnectionInfo().getProperties().isEmpty());
        Assert.assertEquals(SupportedFormat.PARQUET, conn.getConnectionInfo().getFormat());

        conn = (SparkConnection)DriverManager
                .getConnection("com.zensolution.jdbc.spark:/Users/lubx/temp/console?format=csv&timezone=GMT");
        Assert.assertEquals("/Users/lubx/temp/console", conn.getConnectionInfo().getPath());
        Assert.assertEquals(2, conn.getConnectionInfo().getProperties().size());
        Assert.assertEquals("csv", conn.getConnectionInfo().getProperties().getProperty("format"));
        Assert.assertEquals("GMT", conn.getConnectionInfo().getProperties().getProperty("timezone"));
        Assert.assertEquals(SupportedFormat.CSV, conn.getConnectionInfo().getFormat());


        assertThrows(IllegalArgumentException.class, () -> {
            DriverManager.getConnection("com.zensolution.jdbc.spark:/Users/lubx/temp/console?format=NA&timezone=GMT");
        });
    }

    @Test
    public void testConnection() throws Exception {
        Class.forName("com.zensolution.jdbc.spark.SparkDriver");
        File root = new File(this.getClass().getClassLoader().getResource("samples/userdata1").toURI());

        Connection conn = DriverManager.getConnection("com.zensolution.jdbc.spark:"+root.getParentFile().getAbsolutePath());
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("select * from userdata1");
        Assert.assertEquals(13, rs.getMetaData().getColumnCount());
        int count = 0;
        while ( rs.next() ) {
            if ( rs.getInt("id") == 10 ) {
                Assert.assertEquals("Emily", rs.getString("first_name"));
                Assert.assertEquals("Stewart", rs.getString("last_name"));
                Assert.assertEquals("", rs.getString("comments"));
                Assert.assertEquals(Timestamp.valueOf("2016-02-03 18:29:47"), rs.getTimestamp("registration_dttm"));
            }
            count++;

        }
        Assert.assertEquals(1000, count);
    }
}

