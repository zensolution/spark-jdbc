package com.zensolution.jdbc.parquet;

import junit.framework.Assert;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class ParquetConnectionTest {

    @Test
    public void testConnectionInfo() throws Exception {
        Class.forName("com.zensolution.jdbc.parquet.ParquetDriver");
        ParquetConnection conn = (ParquetConnection)DriverManager
                .getConnection("com.zensolution.jdbc.parquet:/Users/lubx/temp/console");
        Assert.assertEquals("/Users/lubx/temp/console", conn.getConnectionInfo().getPath());
        Assert.assertTrue(conn.getConnectionInfo().getProperties().isEmpty());

        conn = (ParquetConnection)DriverManager
                .getConnection("com.zensolution.jdbc.parquet:/Users/lubx/temp/console?format=csv&timezone=GMT");
        Assert.assertEquals("/Users/lubx/temp/console", conn.getConnectionInfo().getPath());
        Assert.assertEquals(2, conn.getConnectionInfo().getProperties().size());
        Assert.assertEquals("csv", conn.getConnectionInfo().getProperties().getProperty("format"));
        Assert.assertEquals("GMT", conn.getConnectionInfo().getProperties().getProperty("timezone"));
    }

    /*
    @Test
    public void testConnection() throws Exception {
        Class.forName("com.zensolution.jdbc.parquet.ParquetDriver");
        Connection conn = DriverManager.getConnection("com.zensolution.jdbc.parquet:/Users/lubx/temp/console");
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("select * from d_organizations_hist");
        if ( rs.next() ) {
            for ( int i=0; i<rs.getMetaData().getColumnCount(); i++) {
                System.out.print(rs.getMetaData().getColumnName(i+1)+": ");
                System.out.println(rs.getObject(i+1));
            }
        }
    }
     */
}

