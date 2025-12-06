package org.example;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Main {
    public static void main(String[] args) {
        // ================================
        // Désactiver tous les logs
        // ================================
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("io.netty").setLevel(Level.OFF);
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF);
        Logger.getLogger("org.eclipse.jetty").setLevel(Level.OFF);
        Logger.getRootLogger().setLevel(Level.OFF);

        // ================================
        // Créer SparkSession
        // ================================
        SparkSession spark = SparkSession.builder()
                .appName("BikeSharingAnalysis")
                .master("local[*]")
                .getOrCreate();

        // ================================
        // 1. Data Loading & Exploration
        // ================================
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/bike_sharing.csv");

        // 1.2 Afficher le schema
        System.out.println("===== Schema =====");
        df.printSchema();

        // 1.3 Afficher les 5 premières lignes
        System.out.println("===== First 5 rows =====");
        df.show(5);

        // 1.4 Nombre total de locations
        System.out.println("===== Total rentals =====");
        System.out.println("Total rentals: " + df.count());

        // ================================
        // 2. Create a Temporary View
        // ================================
        df.createOrReplaceTempView("bike_rentals_view");

        // ================================
        // 3. Basic SQL Queries
        // ================================

        // 3.1 List all rentals longer than 30 minutes
        System.out.println("===== Rentals longer than 30 minutes =====");
        Dataset<Row> longRentals = spark.sql(
                "SELECT * FROM bike_rentals_view WHERE duration_minutes > 30"
        );
        longRentals.show();

        // 3.2 Show all rentals starting at 'Station A'
        System.out.println("===== Rentals starting at Station A =====");
        Dataset<Row> stationA = spark.sql(
                "SELECT * FROM bike_rentals_view WHERE start_station = 'Station A'"
        );
        stationA.show();

        // 3.3 Calculate total revenue
        System.out.println("===== Total revenue =====");
        Dataset<Row> totalRevenue = spark.sql(
                "SELECT SUM(price) as total_revenue FROM bike_rentals_view"
        );
        totalRevenue.show();

        // ================================
        // 4. Aggregation Queries
        // ================================

        // 4.1 Count rentals per start station
        System.out.println("===== Rentals count per start station =====");
        Dataset<Row> rentalsByStation = spark.sql(
                "SELECT start_station, COUNT(*) as total_rentals " +
                        "FROM bike_rentals_view " +
                        "GROUP BY start_station"
        );
        rentalsByStation.show();

        // 4.2 Average rental duration per start station
        System.out.println("===== Average rental duration per start station =====");
        Dataset<Row> avgDuration = spark.sql(
                "SELECT start_station, AVG(duration_minutes) as avg_duration " +
                        "FROM bike_rentals_view " +
                        "GROUP BY start_station"
        );
        avgDuration.show();

        // 4.3 Station with highest number of rentals
        System.out.println("===== Most popular start station =====");
        Dataset<Row> mostPopularStation = spark.sql(
                "SELECT start_station, COUNT(*) as total_rentals " +
                        "FROM bike_rentals_view " +
                        "GROUP BY start_station " +
                        "ORDER BY total_rentals DESC " +
                        "LIMIT 1"
        );
        mostPopularStation.show();

        // ================================
        // 5. Time-Based Analysis
        // ================================

        // 5.1 Extract hour from start_time
        Dataset<Row> dfWithHour = df.withColumn("start_hour", functions.hour(df.col("start_time")));
        dfWithHour.createOrReplaceTempView("bike_rentals_view_hour");

        // 5.2 Count rentals per hour
        System.out.println("===== Rentals per hour =====");
        Dataset<Row> rentalsByHour = spark.sql(
                "SELECT start_hour, COUNT(*) as total_rentals " +
                        "FROM bike_rentals_view_hour " +
                        "GROUP BY start_hour " +
                        "ORDER BY start_hour"
        );
        rentalsByHour.show();

        // 5.3 Most popular start station during morning (7–12)
        System.out.println("===== Most popular station in morning (7-12) =====");
        Dataset<Row> popularMorningStation = spark.sql(
                "SELECT start_station, COUNT(*) as total_rentals " +
                        "FROM bike_rentals_view_hour " +
                        "WHERE start_hour BETWEEN 7 AND 12 " +
                        "GROUP BY start_station " +
                        "ORDER BY total_rentals DESC " +
                        "LIMIT 1"
        );
        popularMorningStation.show();

        // ================================
        // 6. User Behavior Analysis
        // ================================

        // 6.1 Average age of users
        System.out.println("===== Average age of users =====");
        Dataset<Row> avgAge = spark.sql(
                "SELECT AVG(age) as avg_age FROM bike_rentals_view"
        );
        avgAge.show();

        // 6.2 Count users by gender
        System.out.println("===== Users count by gender =====");
        Dataset<Row> countByGender = spark.sql(
                "SELECT gender, COUNT(*) as total_users " +
                        "FROM bike_rentals_view " +
                        "GROUP BY gender"
        );
        countByGender.show();

        // 6.3 Most active age group
        System.out.println("===== Most active age group =====");
        Dataset<Row> ageGroupRentals = spark.sql(
                "SELECT " +
                        "CASE " +
                        "WHEN age BETWEEN 18 AND 30 THEN '18-30' " +
                        "WHEN age BETWEEN 31 AND 40 THEN '31-40' " +
                        "WHEN age BETWEEN 41 AND 50 THEN '41-50' " +
                        "ELSE '51+' END as age_group, " +
                        "COUNT(*) as total_rentals " +
                        "FROM bike_rentals_view " +
                        "GROUP BY age_group " +
                        "ORDER BY total_rentals DESC"
        );
        ageGroupRentals.show();

        // ================================
        // Stop SparkSession
        // ================================
        spark.stop();
    }
}
