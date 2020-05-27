package com.zensolution.jdbc.parquet;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class ParquetConnectionTest {

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
}

