

#  Analyse des locations de vélos 

## **1. Présentation du projet**

L’objectif de ce projet est d’analyser l’utilisation d’un système de location de vélos en libre-service dans une ville. Les données se présentent sous la forme d’un fichier CSV `bike_sharing.csv` contenant des informations sur chaque location :

* **rental_id** : identifiant unique de la location
* **user_id** : identifiant unique de l’utilisateur
* **age** : âge de l’utilisateur
* **gender** : genre de l’utilisateur (M/F)
* **start_time** : date et heure de début de location
* **end_time** : date et heure de fin de location
* **start_station** : station de départ
* **end_station** : station d’arrivée
* **duration_minutes** : durée de la location en minutes
* **price** : prix de la location en dollars

Le projet utilise **Apache Spark** pour traiter et analyser ces données et peut être exécuté en **local** ou sur un **cluster avec HDFS**.

---

## **2. Structure du projet**

* `src/main/java/org/example/Main.java` : fichier principal contenant le code Spark.
* `src/main/resources/bike_sharing.csv` : fichier CSV avec les données de location.
* `pom.xml` : fichier Maven pour gérer les dépendances Spark.

---

## **3. Étapes de traitement et analyse**

### **3.1 Chargement des données**

1. Spark lit le fichier CSV via `SparkSession` :

```java
Dataset<Row> df = spark.read()
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/bike_sharing.csv");
```

* `header` = true → la première ligne contient les noms de colonnes.
* `inferSchema` = true → Spark détecte automatiquement le type de chaque colonne.

2. Affichage du **schema** et des **5 premières lignes** pour vérifier le contenu.

3. Comptage du **nombre total de locations** avec `df.count()`.

---

### **3.2 Création d’une vue SQL temporaire**

```java
df.createOrReplaceTempView("bike_rentals_view");
```

* Permet d’utiliser **Spark SQL** pour interroger les données comme une base de données relationnelle.

---

### **3.3 Requêtes SQL de base**

1. **Locations supérieures à 30 minutes**

```java
SELECT * FROM bike_rentals_view WHERE duration_minutes > 30
```

2. **Locations commençant à “Station A”**

```java
SELECT * FROM bike_rentals_view WHERE start_station = 'Station A'
```

3. **Revenu total généré**

```java
SELECT SUM(price) as total_revenue FROM bike_rentals_view
```

---

### **3.4 Agrégations par station**

1. **Nombre de locations par station de départ**

```java
SELECT start_station, COUNT(*) as total_rentals
FROM bike_rentals_view
GROUP BY start_station
```

2. **Durée moyenne des locations par station**

```java
SELECT start_station, AVG(duration_minutes) as avg_duration
FROM bike_rentals_view
GROUP BY start_station
```

3. **Station la plus populaire**

```java
SELECT start_station, COUNT(*) as total_rentals
FROM bike_rentals_view
GROUP BY start_station
ORDER BY total_rentals DESC
LIMIT 1
```

---

### **3.5 Analyse temporelle**

1. Extraction de l’heure de début de location :

```java
Dataset<Row> dfWithHour = df.withColumn("start_hour", functions.hour(df.col("start_time")));
dfWithHour.createOrReplaceTempView("bike_rentals_view_hour");
```

2. **Nombre de locations par heure** → identifier les **heures de pointe** :

```java
SELECT start_hour, COUNT(*) as total_rentals
FROM bike_rentals_view_hour
GROUP BY start_hour
ORDER BY start_hour
```

3. **Station la plus populaire le matin (7h–12h)** :

```java
SELECT start_station, COUNT(*) as total_rentals
FROM bike_rentals_view_hour
WHERE start_hour BETWEEN 7 AND 12
GROUP BY start_station
ORDER BY total_rentals DESC
LIMIT 1
```

---

### **3.6 Analyse du comportement des utilisateurs**

1. **Âge moyen des utilisateurs** :

```java
SELECT AVG(age) as avg_age FROM bike_rentals_view
```

2. **Nombre d’utilisateurs par genre** :

```java
SELECT gender, COUNT(*) as total_users
FROM bike_rentals_view
GROUP BY gender
```

3. **Groupe d’âge le plus actif** :

```java
SELECT CASE
        WHEN age BETWEEN 18 AND 30 THEN '18-30'
        WHEN age BETWEEN 31 AND 40 THEN '31-40'
        WHEN age BETWEEN 41 AND 50 THEN '41-50'
        ELSE '51+' END as age_group,
        COUNT(*) as total_rentals
FROM bike_rentals_view
GROUP BY age_group
ORDER BY total_rentals DESC
```

---

## **4. Exécution avec HDFS**

Pour exécuter le projet sur HDFS :

1. Copier le fichier CSV dans HDFS :

```bash
hdfs dfs -mkdir -p /user/ayoub/bike_data
hdfs dfs -put bike_sharing.csv /user/ayoub/bike_data/
```

2. Modifier le code pour lire depuis HDFS :

```java
.csv("hdfs://localhost:9000/user/ayoub/bike_data/bike_sharing.csv")
```

3. Compiler le projet avec Maven pour générer le JAR :

```bash
mvn clean package
```

4. Exécuter le JAR avec `spark-submit` :

```bash
spark-submit --class org.example.Main --master local[*] target/SparkSQLBikeSharing-1.0-SNAPSHOT.jar
```

---
hhhhhh

## 5. Compiler le projet en JAR

Avec Maven :

mvn clean package


Le JAR généré se trouve dans :

target/SparkSQLBikeSharing-1.0-SNAPSHOT.jar

## 6. Exécuter le JAR sur le cluster Spark

Commande pour lancer le programme sur le cluster :

spark-submit \
  --class org.example.Main \
  --master spark://<master-host>:7077 \
  --deploy-mode client \
  target/SparkSQLBikeSharing-1.0-SNAPSHOT.jar


--deploy-mode client : l’application démarre depuis la machine où la commande est exécutée.

--deploy-mode cluster : l’application s’exécute entièrement sur le cluster, sans dépendance à la machine cliente.

## 7. Résultats attendus

Identification des stations les plus utilisées.

Détermination des heures de pointe.

Analyse du profil des utilisateurs (âge, genre, groupe le plus actif).

Calcul du revenu total généré par le système de vélos.
