package org.example.exercise;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Scanner;

public class CourseDetailsApp {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:\\hadoop");
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

        SparkConf conf=new SparkConf()
                .setAppName("Demo")
                .setMaster("local[*]");


        JavaSparkContext spark=new JavaSparkContext(conf);

        JavaRDD<String> courseID=spark.textFile("src/main/resources/viewing figures/chapters.csv");
        courseID
                .map(sentence -> {
                            String[] columns=sentence.split(",");
                            Integer chapters=Integer.parseInt(columns[0]);
                            String courseId=columns[1];
                            return new Tuple2<String, Integer>(courseId,chapters);
                        }
                )
                .mapToPair(value -> new Tuple2<String,Integer>(value._1,1))
                .reduceByKey((value1,value2) -> (value1 + value2) )
                .sortByKey()
              //.map(sentence -> new Tuple2<Integer,String>(sentence._2,sentence._1))

                .collect().forEach(value -> System.out.println("Course Id " + value._1 + " Chapters - " + value._2 ));
/*        courseID
                .map(sentence -> {
                    String[] columns=sentence.split(",");
                    Integer chapters=Integer.parseInt(columns[0]);
                    String courseId=columns[1];
                    return new Tuple2<String, Integer>(courseId,chapters);
                }
                )
                .mapToPair(value -> new Tuple2<String,Integer>(value._1,1))
                .reduceByKey((value1,value2) -> (value1 + value2) )

                .map(sentence -> new Tuple2<Integer,String>(sentence._2,sentence._1))

                .collect().forEach(value -> System.out.println("Course Id " + value._1 + " Chapters - " + value._2 ));*/
        Scanner scanner=new Scanner(System.in);
        scanner.next();
        System.out.println("hi");

    }
}
