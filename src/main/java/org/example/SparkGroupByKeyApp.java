package org.example;

import com.google.common.collect.Iterables;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkGroupByKeyApp {
    public static void main(String[] args) {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

        SparkConf conf=new SparkConf()
                .setAppName("Demo")
                .setMaster("local[*]");


        JavaSparkContext spark=new JavaSparkContext(conf);

        JavaRDD<Integer> data = spark.parallelize(Arrays.asList(10,20,30,40,40,30));
        data.collect().forEach(System.out::println);

   data
                .mapToPair(value -> new Tuple2<>(value,1))
                .groupByKey()
                .collect()
                .forEach(value -> System.out.println(" Hurray " + value._1 + " " + Iterables.size(value._2)));

    }
}
