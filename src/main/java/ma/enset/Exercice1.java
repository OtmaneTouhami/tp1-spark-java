package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Exercice1 {

    public static void main(String[] args) {
        // Configuration de Spark
        SparkConf conf = new SparkConf().setAppName("Ventes RDD Analysis").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Charger le fichier de données
        JavaRDD<String> lines = sc.textFile("data/ventes.txt");

        // --- Question 1: Calculer le total des ventes par ville ---
        System.out.println("--- Total des ventes par ville ---");
        JavaPairRDD<String, Double> ventesParVille = lines.mapToPair(line -> {
            String[] parts = line.split(" ");
            String ville = parts[1];
            double prix = Double.parseDouble(parts[3]);
            return new Tuple2<>(ville, prix);
        });

        JavaPairRDD<String, Double> totalParVille = ventesParVille.reduceByKey(Double::sum);

        totalParVille.foreach(data -> System.out.println("Ville: " + data._1() + ", Total des ventes: " + String.format("%.2f", data._2())));


        // --- Question 2: Calculer le prix total des ventes par ville et par année ---
        System.out.println("\n--- Total des ventes par ville et par année ---");
        JavaPairRDD<Tuple2<String, String>, Double> ventesParAnneeVille = lines.mapToPair(line -> {
            String[] parts = line.split(" ");
            String annee = parts[0].substring(0, 4); // Extrait l'année
            String ville = parts[1];
            double prix = Double.parseDouble(parts[3]);
            // Clé composite (année, ville)
            return new Tuple2<>(new Tuple2<>(annee, ville), prix);
        });

        JavaPairRDD<Tuple2<String, String>, Double> totalParAnneeVille = ventesParAnneeVille.reduceByKey(Double::sum);

        totalParAnneeVille.foreach(data -> System.out.println("Année: " + data._1()._1() + ", Ville: " + data._1()._2() + ", Total des ventes: " + String.format("%.2f", data._2())));


        // Fermer le contexte Spark
        sc.close();
    }
}