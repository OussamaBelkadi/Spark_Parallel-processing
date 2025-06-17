package com.example.sprak_tp_sp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class TotalVentesParVille {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Total Ventes par Ville").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rddLines = sc.textFile("ventes.txt");
        JavaRDD<String[]> rddData = rddLines.map(line -> line.split(" "));

        // On suppose que les champs sont dans l'ordre : date, ville, produit, prix
        JavaPairRDD<String, Double> ventesParVille = rddData.mapToPair(data -> new Tuple2<>(data[1], Double.parseDouble(data[3])))
                .reduceByKey((a, b) -> a + b);

        ventesParVille.foreach(elem -> System.out.println("Ville : " + elem._1 + ", Total Ventes : " + elem._2));
    }
}
