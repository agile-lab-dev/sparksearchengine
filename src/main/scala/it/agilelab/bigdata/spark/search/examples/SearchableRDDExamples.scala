package it.agilelab.bigdata.spark.search.examples

import it.agilelab.bigdata.spark.search.SearchableRDD
import it.agilelab.bigdata.spark.search.utils.WikipediaXmlDumpParser.xmlDumpToRdd
import it.agilelab.bigdata.spark.search.utils.wikipage
import org.apache.spark.{SparkConf, SparkContext}

object SearchableRDDExamples {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("SearchableRDD examples")
			.setMaster("local[4]")
		val sc = new SparkContext(conf)
		
		// paths
		val xmlPath = args(0) // input dump file path
		val outputPath = args(1) // output directory path
		
		// read xml dump into an rdd of wikipages
		val wikipages = xmlDumpToRdd(sc, xmlPath)
		
		// index
		val searchable: SearchableRDD[wikipage] =
	}
}