package it.agilelab.bigdata.spark.search.evaluation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext


trait SparkApplication {
	def conf = new SparkConf()
		.set("spark.app.name", "[SPARKSEARCH]")
		//.set("spark.master", "local[*]")
		//.set("spark.driver.memory", "44G")
		.set("spark.driver.maxResultSize", "4G")
		.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		.set("spark.kryoserializer.buffer.max", "512m")
		//.set("spark.kryo.registrationRequired", "true")
		.registerKryoClasses(Array(classOf[Array[String]]))
	
	val sc = SparkContext.getOrCreate(conf)
	val hadoopConfiguration = sc.hadoopConfiguration
	val sqlContext = SQLContext.getOrCreate(sc)
}
