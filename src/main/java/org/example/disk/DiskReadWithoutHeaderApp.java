package org.example.disk;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class DiskReadWithoutHeaderApp {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:\\hadoop");
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

        SparkConf conf=new SparkConf()
                .setAppName("Demo")
                .setMaster("local[*]");


        JavaSparkContext spark=new JavaSparkContext(conf);

        JavaRDD<String> readmeRDD=spark.textFile("src/main/resources/exams/emp.txt");
        String header=readmeRDD.first();
        readmeRDD
                .filter(row -> ! row.equalsIgnoreCase(header))
                .collect().forEach(value -> System.out.println(" " + value ));

    }
}
