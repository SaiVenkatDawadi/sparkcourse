package com.jobreadyprogrammer.spark;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import static org.apache.spark.sql.functions.*;

public class Application {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		
		SparkSession spark = SparkSession.builder()
		        .appName("Combine 2 Datasets")
		        .master("local")
		        .getOrCreate();

		Dataset<Row> durhamDf = buildDurhamParksDF(spark);
		durhamDf.show(10);
		System.out.println(durhamDf.count());
		Dataset<Row> philDf = buildphilParksDF(spark);
		philDf.show(10);
		System.out.println(philDf.count());
		Dataset<Row> combinedDF = combineDataFrames(durhamDf, philDf);
		System.out.println(combinedDF.count());
		combinedDF.show(100);
		Partition[] partitions = combinedDF.rdd().partitions();
		System.out.println(partitions.length);
	}

	public static Dataset<Row> buildDurhamParksDF(SparkSession spark) {
		Dataset<Row> df = spark.read().format("json").option("multiline", true).load("src/main/resources/durham-parks.json");

		df = df.withColumn("park_id", concat(df.col("datasetid"), lit("_"), df.col("fields.objectid"), lit("_Durham")))
				.withColumn("park_name", df.col("fields.park_name"))
				.withColumn("city", lit("Durham"))
				.withColumn("address", df.col("fields.address"))
				.withColumn("hasPlayGround", df.col("fields.playground"))
				.withColumn("zipCode", df.col("fields.zip"))
				.withColumn("land_in_acres", df.col("fields.acres"))
				.withColumn("geoX", df.col("geometry.coordinates").getItem(0))
				.withColumn("geoY", df.col("geometry.coordinates").getItem(1))
				.drop("fields").drop("geometry").drop("record_timestamp").drop("recordid").drop("datasetid");
		return df;
	}

	public static Dataset<Row> buildphilParksDF(SparkSession spark) {
		Dataset<Row> df = spark.read()
				.format("csv")
				.option("multiline", true)
				.option("header", true)
				.load("src/main/resources/philadelphia_recreations.csv");

		df = df.filter(lower(df.col("USE_")).like("%park%"));

		df = df.withColumn("park_id",
						concat(df.col("OBJECTID"), lit("_Philadelphia")))
				.withColumnRenamed("ASSET_NAME", "park_name")
				.withColumn("city", lit("Philadelphia"))
				.withColumnRenamed("ADDRESS","address")
				.withColumn("hasPlayGround", lit("UNKNOWN"))
				.withColumnRenamed("ZIPCODE", "zipCode")
				.withColumnRenamed("ACREAGE","land_in_acres")
				.withColumn("geoX", lit("UNKNOWN"))
				.withColumn("geoY", lit("UNKNOWN"))
				.drop("SITE_NAME")
				.drop("OBJECTID")
				.drop("CHILD_OF")
				.drop("TYPE")
				.drop("USE_")
				.drop("DESCRIPTION")
				.drop("ALLIAS")
				.drop("CHRONOLOGY")
				.drop("NOTES")
				.drop("DATE_EDITED")
				.drop("EDITED_BY")
				.drop("OCCUPANT")
				.drop("TENANT")
				.drop("LABEL")
				.drop("SQ_FEET");
		return df;
	}

	public static Dataset<Row> combineDataFrames(Dataset<Row> df1, Dataset<Row> df2) {
		Dataset<Row> combinedDf = df1.unionByName(df2).repartition(5);
		return combinedDf;
	}
}