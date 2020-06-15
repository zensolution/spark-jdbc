# Spark JDBC driver

## Build

I haven't gotten chance to publish it into Maven Central. For now, please use the following command to build a fat Jar 

````
gradlew clean shadowJar
````

## Documentation

Spark JDBC driver is a read-only JDBC driver that uses Spark SQL as database tables. It is ideal for .

The URL syntax for the driver URL is as follows

    com.zensolution.jdbc.spark:<foldername>[?<property=value]",

