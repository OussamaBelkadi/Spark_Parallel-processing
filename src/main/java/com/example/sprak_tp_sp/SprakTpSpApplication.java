package com.example.sprak_tp_sp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import scala.Tuple2;

import java.util.Arrays;

@SpringBootApplication
public class SprakTpSpApplication {

    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("Word Count").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);

        JavaRDD<String> rddLines=sc.textFile("words.txt");
        JavaRDD<String> rddWords=rddLines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String,Integer> wordsPairRdd=rddWords.mapToPair(word -> new Tuple2<>(word,1));
        JavaPairRDD<String,Integer> wordWordCount= wordsPairRdd.reduceByKey((elem, accuml) -> elem+accuml);
        wordWordCount.foreach(elem -> System.out.println(elem._1+" "+elem._2));
    }

}
