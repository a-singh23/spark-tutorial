package org.example.stream;


import org.apache.commons.io.FileUtils;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.File;


public class StreamWithWindowing {
    static JavaStreamingContext stream=null;
    static FileAlterationMonitor monitor =null;

    public static void watchDog() throws Exception {
        String DIR="C:\\Users\\a_sin\\Desktop\\SPARK-WAREHOUSE";

        File parentDirectory = FileUtils.getFile(DIR);
        FileAlterationObserver observer = new
                FileAlterationObserver(parentDirectory);

        observer.addListener(new FileAlterationListenerAdaptor() {
            @Override
            public void onDirectoryCreate(File file) {
                System.out.println("Folder created: " + file.getName());
            }
            @Override
            public void onDirectoryDelete(File file) {
                System.out.println("Folder deleted: " + file.getName());
            }
            @Override
            public void onFileCreate(File file) {
                System.out.println("Stop File created: " + file.getName());
                stream.close();
                System.exit(1);   //Exiting the Application run
            }
            @Override
            public void onFileDelete(File file) {
                System.out.println("File deleted: " + file.getName());
            }
        });
        monitor = new FileAlterationMonitor(500, observer);

    }

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir","C:\\hadoop");
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkConf conf=new SparkConf()
                .setAppName("demo-Spark")
                .set("spark.streaming.stopGracefullyOnShutdown","true")
                .setMaster("local[*]");

        stream=new JavaStreamingContext(conf, Durations.seconds(60));

        JavaReceiverInputDStream<String> messages= stream.socketTextStream("localhost",8989);
        JavaDStream<String>everyMessage= messages.map(value -> value);
        everyMessage.print();

        JavaPairDStream<String,Integer> everyMessageN=everyMessage
                .mapToPair(level ->  new Tuple2<>(level.split(",")[0],1))
                        .reduceByKeyAndWindow((val1,val2)-> val1 + val2,Durations.minutes(1));

        everyMessageN.print();

        watchDog();

        stream.start();
        monitor.start();
        stream.awaitTermination();


    }
}
