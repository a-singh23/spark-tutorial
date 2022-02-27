package org.example.udf;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;


public class UDF1App {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:\\hadoop");
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

        SparkSession session = SparkSession.builder()
                .appName("Spark sql demo")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","C:\\spark-warehouse")
                .getOrCreate();

        Dataset<Row> students=session.read()
                        .option("header",true)
                .option("inferSchema",true)
                .csv("src/main/resources/exams/students.csv");

       session.udf().register("checkIfPass",score -> {
           if(score.equals("A+") || score.equals("A") || score.equals("B") || score.equals("C"))
               return "PASS";
            return "FAIL";
       }, DataTypes.StringType);

       students
                .withColumn("REMARKS",callUDF("checkIfPass",col("grade")))
                .show(20);
    }
}
