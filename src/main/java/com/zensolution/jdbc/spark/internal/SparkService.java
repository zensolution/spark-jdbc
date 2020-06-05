package com.zensolution.jdbc.spark.internal;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SparkService {
    protected SparkSession buildSparkSession(ConnectionInfo info) {
        SparkSession.Builder builder = SparkSession.builder().master("local").appName("parquet-jdbc-driver")
                .config("spark.sql.session.timeZone", "UTC");
        if (info.getFormat() == SupportedFormat.DELTA) {
            builder = prepareAwsCredential(builder);
        }
        return builder.getOrCreate();
    }

    private SparkSession.Builder prepareAwsCredential(SparkSession.Builder builder) {
        String accessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
        String secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY");
        String sessionToken = System.getenv("AWS_SESSION_TOKEN");

        if ( accessKeyId!=null && !accessKeyId.isEmpty() ) {
            builder = builder.config("spark.hadoop.fs.s3a.access.key", accessKeyId)
                    .config("spark.hadoop.fs.s3a.secret.key", secretAccessKey)
                    .config("spark.hadoop.fs.s3a.session.token", sessionToken)
                    .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
                    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
                    .config("spark.hadoop.fs.s3a.server-side-encryption-algorithm","AES256");
        }
        return builder;
    }

    public Dataset<Row> executeQuery(ConnectionInfo info, String sqlText) throws ParseException {
        SparkSession spark = buildSparkSession(info);
        prepareTempView(info, sqlText);
        return spark.sql(sqlText);
    }

    public void prepareTempView(ConnectionInfo info, String sqlText) throws ParseException {
        Map<String, String> options = info.getProperties().entrySet().stream().collect(
                Collectors.toMap(
                        e -> e.getKey().toString(),
                        e -> e.getValue().toString()
                )
        );

        SparkSession spark = buildSparkSession(info);
        Set<String> tables = getRelations(spark.sessionState().sqlParser().parsePlan(sqlText));
        tables.forEach(table -> {
            SupportedFormat format = info.getFormat();
            Dataset<Row> ds = spark.read().format(format.name().toLowerCase(Locale.getDefault()))
                    .options(options)
                    .load(format.getSparkPath(info.getPath(), table));
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
