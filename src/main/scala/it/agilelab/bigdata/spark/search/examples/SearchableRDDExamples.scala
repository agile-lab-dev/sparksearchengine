package it.agilelab.bigdata.spark.search.examples

import it.agilelab.bigdata.spark.search.SearchableRDD
import it.agilelab.bigdata.spark.search.dsl._
import it.agilelab.bigdata.spark.search.impl.analyzers.EnglishWikipediaAnalyzer
import it.agilelab.bigdata.spark.search.impl.queries.DefaultQueryConstructor
import it.agilelab.bigdata.spark.search.impl.{DistributedIndexLuceneRDD, LuceneConfig}
import it.agilelab.bigdata.spark.search.utils.WikipediaXmlDumpParser.xmlDumpToRdd
import it.agilelab.bigdata.spark.search.utils.wikipage
import org.apache.spark.{SparkConf, SparkContext}

object SearchableRDDExamples {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("SearchableRDD example")
			.setMaster("local[6]")
		val sc = new SparkContext(conf)
		
		// paths
		val xmlPath = args(0) // input dump file path
		
		// read xml dump into an rdd of wikipages
		val wikipages = xmlDumpToRdd(sc, xmlPath).cache()
		
		// define a configuration to use english analyzers for wikipedia and the default query constructor
		val luceneConfig = LuceneConfig(classOf[EnglishWikipediaAnalyzer],
		                                classOf[EnglishWikipediaAnalyzer],
		                                classOf[DefaultQueryConstructor])
		
		// index using DistributedIndexLuceneRDD implementation with 5 indices
		val searchable: SearchableRDD[wikipage] = DistributedIndexLuceneRDD(wikipages, 5, luceneConfig).cache()
		
		// define a query using the DSL
		val query = "text" matchAll termSet("tropical","island")
		
		// run it against the searchable rdd
		val results = searchable.aggregatingSearch(query, 10)
		
		// print results
		println(s"Results for query $query:")
		results foreach { result => println(f"score: ${result._2}%6.3f title: ${result._1.title}") }
		
		// define query generator
		val queryGenerator: wikipage => DslQuery = (wp) => "text" matchText wp.title
		
		// do a query join on itself
		val join = searchable.queryJoin(searchable, queryGenerator, 10) map {
			case (wp, results) => (wp, results map { case (wp2, score) => (wp2.title, score) })
		}
		
		// print first five elements and corresponding matches
		println("Results for query join:")
		join.take(5) foreach {
			case (wp, results) =>
				println(s"title: ${wp.title}")
				results foreach { result => println(f"\tscore: ${result._2}%6.3f title: ${result._1}") }
		}
	}
}