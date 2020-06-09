package com.zensolution.jdbc.spark.provider;

import com.zensolution.jdbc.spark.internal.ConnectionInfo;

import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;

public interface SparkConfProvider {

    static final String SPARK_CONF_PROVIDER = "sparkconf.provider.class";

    Map<String, String> getSparkConf(ConnectionInfo connectionInfo) throws SQLException;

    static Optional<SparkConfProvider> getSparkConfProvider(ConnectionInfo connectionInfo) throws SQLException {
        String provider = connectionInfo.getProperties().getProperty(SPARK_CONF_PROVIDER);
        if ( provider == null ) {
            return Optional.empty();
        } else {
            try {
                Class clz = Class.forName(provider);
                return Optional.of((SparkConfProvider)clz.newInstance());
            } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
                throw new SQLException(e);
            }
        }
     }

}
