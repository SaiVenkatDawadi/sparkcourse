package com.jobreadyprogrammer.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class Application {
	public static void main(String[] args) {

		SparkSession spark = new SparkSession.Builder().appName("CSV to DB").master("local").getOrCreate();

		Dataset<Row> df = spark.read().format("csv").option("header", true).load("src/main/resources/name_and_comments.txt");

		df = df.withColumn("full_name", concat(df.col("last_name"), lit(", "), df.col("first_name")));

		df = df.filter(df.col("comment").rlike("\\d+")).orderBy(df.col("last_name").asc());

		df.show();
	}
}