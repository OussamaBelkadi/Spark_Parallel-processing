package com.example.sprak_tp_sp;

import org.apache.spark.SparkConf;

public class PrixTotalVentesParVilleEtAnnee {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Prix Total Ventes par Ville et Année").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rddLines = sc.textFile("ventes.txt");
        JavaRDD<String[]> rddData = rddLines.map(line -> line.split(" "));

        // On suppose que les champs sont dans l'ordre : date, ville, produit, prix
        // On extrait l'année de la date
        JavaPairRDD<Tuple2<String, Integer>, Double> ventesParVilleEtAnnee = rddData.mapToPair(data -> {
            String[] dateParts = data[0].split("-");
            int annee = Integer.parseInt(dateParts[0]);
            return new Tuple2<>(new Tuple2<>(data[1], annee), Double.parseDouble(data[3]));
        }).reduceByKey((a, b) -> a + b);

        ventesParVilleEtAnnee.foreach(elem -> System.out.println("Ville : " + elem._1._1 + ", Année : " + elem._1._2 + ", Prix Total Ventes : " + elem._2));
    }
}