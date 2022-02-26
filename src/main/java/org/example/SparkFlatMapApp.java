package org.example;

import com.google.common.collect.Iterables;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Array;
import scala.Tuple2;

import java.util.Arrays;

public class SparkFlatMapApp {
    public static void main(String[] args) {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

        SparkConf conf=new SparkConf()
                .setAppName("Demo")
                .setMaster("local[*]");


        JavaSparkContext spark=new JavaSparkContext(conf);

        JavaRDD<String> data = spark.parallelize(Arrays.asList(
                "WARN: Love you brother",
                "INFO: Play life don't replay sorrows",
                "WARN: Rewind good times forward bad ones",
                "ERROR: Hurray",
                "Do what is right",
                "Love you !"));
        data.collect().forEach(System.out::println);

   data
                .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
           .filter(value -> value.contains(":"))
           
                .collect()
                .forEach(value -> System.out.println(value ));

    }
}
