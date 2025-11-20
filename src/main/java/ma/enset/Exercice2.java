package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Exercice2 {

    // Regex pour parser les lignes de log Apache
    private static final String LOG_REGEX = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";
    private static final Pattern PATTERN = Pattern.compile(LOG_REGEX);

    // Classe simple pour stocker les informations extraites d'une ligne de log
    public static class LogEntry implements Serializable {
        String ip;
        String dateTime;
        String method;
        String resource;
        int httpCode;
        long responseSize;

        public LogEntry(String ip, String dateTime, String method, String resource, int httpCode, long responseSize) {
            this.ip = ip;
            this.dateTime = dateTime;
            this.method = method;
            this.resource = resource;
            this.httpCode = httpCode;
            this.responseSize = responseSize;
        }

        public static LogEntry parse(String logLine) {
            Matcher matcher = PATTERN.matcher(logLine);
            if (matcher.find()) {
                return new LogEntry(
                        matcher.group(1),
                        matcher.group(4),
                        matcher.group(5),
                        matcher.group(6),
                        Integer.parseInt(matcher.group(8)),
                        Long.parseLong(matcher.group(9))
                );
            }
            return null; // Retourne null si la ligne ne correspond pas au format
        }
    }


    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Log Analysis").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 1. Lecture des données
        JavaRDD<String> logLines = sc.textFile("data/access.log");

        // 2. Extraction des champs (et mise en cache pour réutilisation)
        JavaRDD<LogEntry> parsedLogs = logLines
                .map(LogEntry::parse)
                .filter(entry -> entry != null) // Filtre les lignes qui n'ont pas pu être parsées
                .cache(); // Mise en cache car ce RDD est utilisé plusieurs fois

        // 3. Statistiques de base
        long totalRequests = parsedLogs.count();
        long totalErrors = parsedLogs.filter(log -> log.httpCode >= 400).count();
        double errorPercentage = (double) totalErrors / totalRequests * 100.0;

        System.out.println("--- Statistiques de base ---");
        System.out.println("Nombre total de requêtes: " + totalRequests);
        System.out.println("Nombre total d'erreurs (code >= 400): " + totalErrors);
        System.out.println("Pourcentage d'erreurs: " + String.format("%.2f%%", errorPercentage));

        // 4. Top 5 des adresses IP
        JavaPairRDD<String, Integer> ipCounts = parsedLogs
                .mapToPair(log -> new Tuple2<>(log.ip, 1))
                .reduceByKey(Integer::sum);

        List<Tuple2<String, Integer>> top5IPs = ipCounts
                .mapToPair(Tuple2::swap) // (count, ip)
                .sortByKey(false) // Trie par nombre de requêtes (décroissant)
                .mapToPair(Tuple2::swap) // (ip, count)
                .take(5);

        System.out.println("\n--- Top 5 des adresses IP ---");
        top5IPs.forEach(tuple -> System.out.println("IP: " + tuple._1() + ", Requêtes: " + tuple._2()));

        // 5. Top 5 des ressources
        JavaPairRDD<String, Integer> resourceCounts = parsedLogs
                .mapToPair(log -> new Tuple2<>(log.resource, 1))
                .reduceByKey(Integer::sum);

        List<Tuple2<String, Integer>> top5Resources = resourceCounts
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .mapToPair(Tuple2::swap)
                .take(5);

        System.out.println("\n--- Top 5 des ressources les plus demandées ---");
        top5Resources.forEach(tuple -> System.out.println("Ressource: " + tuple._1() + ", Vues: " + tuple._2()));

        // 6. Répartition des requêtes par code HTTP
        JavaPairRDD<Integer, Integer> httpCodeCounts = parsedLogs
                .mapToPair(log -> new Tuple2<>(log.httpCode, 1))
                .reduceByKey(Integer::sum)
                .sortByKey(); // Trie par code HTTP

        System.out.println("\n--- Répartition des requêtes par code HTTP ---");
        httpCodeCounts.foreach(tuple -> System.out.println("Code HTTP: " + tuple._1() + ", Nombre de requêtes: " + tuple._2()));

        sc.close();
    }
}