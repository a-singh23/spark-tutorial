package org.example.disk;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.example.utils.Util;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Locale;

public class KeywordRankingApp {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:\\hadoop");
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

        SparkConf conf=new SparkConf()
                .setAppName("Demo")
                .setMaster("local[2]");


        JavaSparkContext spark=new JavaSparkContext(conf);

        JavaRDD<String> readmeRDD=spark
                .textFile("src/main/resources/subtitles/input.txt");

        System.out.println(readmeRDD
                .map(sentence ->
                        sentence
                                .replaceAll("[^a-zA-Z\\s]","")
                                .toLowerCase(Locale.ROOT))
                .filter(sentence -> sentence.trim().length()>1)
                .flatMap(sentence -> Arrays.asList(sentence.split(" ") ).iterator())
                .filter(word -> Util.isNotBoring(word))
                .filter(word -> word.trim().length()>1)
                .mapToPair(word -> new Tuple2<>(word,1))
                .reduceByKey((value1,value2) -> value1 + value2)
                .mapToPair(tuple -> new Tuple2(tuple._2,tuple._1))
                .getNumPartitions());
                ;
        readmeRDD
                .map(sentence ->
                        sentence
                                .replaceAll("[^a-zA-Z\\s]","")
                                .toLowerCase(Locale.ROOT))
                .filter(sentence -> sentence.trim().length()>1)
                .flatMap(sentence -> Arrays.asList(sentence.split(" ") ).iterator())
                .filter(word -> Util.isNotBoring(word))
                .filter(word -> word.trim().length()>1)
                .mapToPair(word -> new Tuple2<>(word,1))
                .reduceByKey((value1,value2) -> value1 + value2)
                .mapToPair(tuple -> new Tuple2(tuple._2,tuple._1))
                .sortByKey(false)
                //.take(10)
                .collect()

                .forEach(value -> System.out.println(" " + value  ));

    }
}
