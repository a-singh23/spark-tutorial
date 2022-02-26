package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.sources.In;
import scala.Int;
import scala.Tuple2;

import java.util.Arrays;

public class SparkApp {
    public static void main(String[] args) {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

        SparkConf conf=new SparkConf()
                .setAppName("Demo")
                .setMaster("local[*]");


        JavaSparkContext spark=new JavaSparkContext(conf);

        JavaRDD<Integer> data = spark.parallelize(Arrays.asList(10,20,30,40));
        data.collect().forEach(System.out::println);

        JavaRDD<Integer> mappedData=data.map(value -> 1);
        Integer count=mappedData.reduce((value1, value2) -> value1 + value2);
        System.out.println("Count: " + count);

        spark.close();
    }
}
