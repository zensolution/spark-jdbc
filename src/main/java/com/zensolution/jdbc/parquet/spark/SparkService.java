package com.zensolution.jdbc.parquet.spark;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import java.io.File;
import java.util.Set;
import java.util.stream.Collectors;

public class SparkService {
    public SparkSession buildSparkSession() {
        return SparkSession.builder().master("local").appName("parquet-jdbc-driver")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();
    }

    public Dataset<Row> executeQuery(String sqlText) {
        SparkSession spark = buildSparkSession();
        return spark.sql(sqlText);
    }

    public void parquet(String path, String sqlText) throws ParseException {
        SparkSession spark = buildSparkSession();
        Set<String> tables = getRelations(spark.sessionState().sqlParser().parsePlan(sqlText));
        tables.forEach(table -> {
            Dataset<Row> ds = spark.read().parquet(new File(path, table).getAbsolutePath());
            ds.createOrReplaceTempView(table);
        });
    }

    private Set<String> getRelations(LogicalPlan plan) {
        return scala.collection.JavaConverters.seqAsJavaListConverter(plan.collectLeaves()).asJava()
                .stream()
                .map(logicalPlan -> {
                    if (logicalPlan instanceof UnresolvedRelation) {
                        return ((UnresolvedRelation) logicalPlan).tableName();
                    }

                    return "";
                }).collect(Collectors.toSet());
    }
}
