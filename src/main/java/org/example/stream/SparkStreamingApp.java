package org.example.stream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


public class SparkStreamingApp {
    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkConf conf=new SparkConf()
            .setAppName("Demo")
            .setMaster("local[*]");

        //Stream context
        JavaStreamingContext spark=new JavaStreamingContext(conf, Durations.seconds(30));

        //Receive Stream
        JavaReceiverInputDStream<String> lines= spark.socketTextStream("localhost",8989);


        //Transformations
        JavaDStream<String>  result= lines.map(val -> val);
        result.print();


        spark.start();
        spark.awaitTermination();
    }
}
