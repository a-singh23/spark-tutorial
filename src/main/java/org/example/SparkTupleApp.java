package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkTupleApp {
    public static void main(String[] args) {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

        SparkConf conf=new SparkConf()
                .setAppName("Demo")
                .setMaster("local[*]");


        JavaSparkContext spark=new JavaSparkContext(conf);

        JavaRDD<Integer> data = spark.parallelize(Arrays.asList(10,20,30,40,40,30));
        data.collect().forEach(System.out::println);

/*
   JavaPairRDD<Integer,Integer> pair=data.mapToPair(value -> new Tuple2<>(value,1));

   JavaPairRDD<Integer,Integer> counts=pair.reduceByKey((value1, value2) -> value1 + value2);

   counts.collect().forEach(value -> System.out.println(" Hurray " + value._1 + " " + value._2));
*/

        //Better way to Code
   data
                .mapToPair(value -> new Tuple2<>(value,1))
                .reduceByKey((value1, value2) -> value1 + value2)
                .collect()
                .forEach(value -> System.out.println(" Hurray " + value._1 + " " + value._2));

    }
}
