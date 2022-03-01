package org.example.stream.structured;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Scanner;


public class StructuredApp {

    public static void main(String[] args) throws InterruptedException, StreamingQueryException {

        System.setProperty("hadoop.home.dir","C:\\hadoop");
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);


        SparkSession session = SparkSession.builder()
                .appName("Spark sql demo")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","C:\\spark-warehouse")
                .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
                .getOrCreate();

        Dataset<Row> logs=session
                .readStream()
                .format("socket")
                .option("host","localhost")
                .option("port","8989")
                .load();

        logs.createOrReplaceTempView("logTbl");

        //logs.printSchema();

        logs=logs
                .withColumn("timestamp", current_timestamp())
                .withWatermark("timestamp","2 minutes");

        logs=session.sql("select  * from logTbl ");



        // Spark Structured Streaming writing to console
        /*
                logs
                    .writeStream()
                    .format("console")
                    .outputMode(OutputMode.Update())
                    .start()
                    .awaitTermination();
         */


        // Spark Structured Streaming writing to csv
        //Data source csv does not support Complete output mode;

                logs
                    .coalesce(1)
                    .writeStream()
                    .format("csv")
                    .option("path","C:\\Users\\a_sin\\Desktop\\SPARK-WAREHOUSE")
                    .option("checkpointLocation","C:\\Users\\a_sin\\Desktop\\checkpoint")
                          //  .option("format",OutputMode.Append())
                            .outputMode(OutputMode.Append())
                    .start()
                    .awaitTermination();

        // Note:
        // Spark Structured Streaming does not support writing the result of a streaming query to a Hive table.
        // Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark;;


    }
}
