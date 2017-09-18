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
			.setMaster("local[8]")
		val sc = new SparkContext(conf)
		
		// paths
		val xmlPath = args(0) // input dump file path
		
		// read xml dump into an rdd of wikipages
		val wikipages = xmlDumpToRdd(sc, xmlPath).cache()
		
		// count pages
		println(s"\n\n\nNumber of pages: ${wikipages.count()}\n\n")
		
		// define a configuration to use english analyzers for wikipedia and the default query constructor
		val luceneConfig = LuceneConfig(classOf[EnglishWikipediaAnalyzer],
		                                classOf[EnglishWikipediaAnalyzer],
		                                classOf[DefaultQueryConstructor])
		
		// index using DistributedIndexLuceneRDD implementation with 2 indices
		val searchable: SearchableRDD[wikipage] = DistributedIndexLuceneRDD(wikipages, 2, luceneConfig).cache()
		
		// define a query using the DSL
		val query = "text" matchAll termSet("island")
		
		// run it against the searchable rdd
		val queryResults = searchable.aggregatingSearch(query, 10)
		
		// print results
		println(s"\n\n\nResults for query $query:")
		queryResults foreach { result => println(f"\tscore: ${result._2}%6.3f title: ${result._1.title}") }
		println("\n\n")
		
		// get information about the indices
		val indicesInfo = searchable.getIndicesInfo
		
		// print it
		println(s"\n\n\n${indicesInfo.prettyToString()}")
		println("\n\n")
		
		// get information about the terms
		val termInfo = searchable.getTermCounts
		
		// print top 10 terms for "title" field
		val topTenTerms = termInfo("title").toList.sortBy(_._2).reverse.take(10)
		println("\n\n\nTop 10 terms for \"title\" field:")
		topTenTerms foreach { case (term, count) => println(s"\tterm: $term count: $count") }
		println("\n\n")
		
		// define query generator where we simply use the title and the first few characters of the text as a query
		val queryGenerator: wikipage => DslQuery = (wp) => "text" matchText (wp.title + wp.text.take(200))
		
		// do a query join on itself
		val join = searchable.queryJoin(searchable, queryGenerator, 5) map {
			case (wp, results) => (wp, results map { case (wp2, score) => (wp2.title, score) })
		}
		val queryJoinResults = join.take(5)
		
		// print first five elements and corresponding matches
		println("\n\n\nResults for query join:")
		queryJoinResults foreach {
			case (wp, results) =>
				println(s"title: ${wp.title}")
				results foreach { result => println(f"\tscore: ${result._2}%6.3f title: ${result._1}") }
		}
		println("\n\n")
	}
}