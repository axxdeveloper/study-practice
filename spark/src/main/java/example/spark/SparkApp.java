package example.spark;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;

import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions.JDBC_CREATE_TABLE_COLUMN_TYPES;
import static org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions.JDBC_DRIVER_CLASS;
import static org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions.JDBC_TABLE_NAME;
import static org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions.JDBC_URL;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.window;

public class SparkApp {

    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = SparkSession.builder().master("local[4]").getOrCreate();

        // generate events
        // each event has an id (eventId) and a timestamp
        // an eventId is a number between 0 an 99
        Dataset<Row> events = getEvents(spark);
        events.printSchema();

        events.writeStream()
            .outputMode(OutputMode.Update())
            .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (rowDataset, batchId) -> {
                Dataset<Row> windowedCounts = rowDataset.groupBy(
                      window(rowDataset.col("timestamp"), "1 minutes"),
                      rowDataset.col("eventId").as("event_id")
                    ).count()
                    .select(expr("uuid() as id"),
                            expr("date_format(window.end, 'yyyyMMddHHmm') as event_time"), 
                            col("event_id"), 
                            col("count"));

                windowedCounts.show();
                windowedCounts
                    .write().format("jdbc")
                    .option(JDBC_CREATE_TABLE_COLUMN_TYPES(), "event_time varchar(64), event_id int, count bigint")
                    .mode(SaveMode.Append)
                    .option(JDBC_URL(), "jdbc:hsqldb:hsql://localhost:9001/xdb")
                    .option(JDBC_TABLE_NAME(), "event_count")
                    .option(JDBC_DRIVER_CLASS(), "org.hsqldb.jdbc.JDBCDriver")
                    .option("user", "sa")
                    .save();
            })
            .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
            .start();
        
        // the stream will run forever
        spark.streams().awaitAnyTermination();
    }

    private static Dataset<Row> getEvents(SparkSession spark) {
        return spark
                .readStream()
                .format("rate")
                .option("rowsPerSecond", "1")
                .load()
                .withColumn("eventId", functions.rand(System.currentTimeMillis()).multiply(functions.lit(100)).cast(DataTypes.LongType))
                .select("eventId", "timestamp");
    }

}
