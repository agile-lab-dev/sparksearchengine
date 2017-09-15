package it.agilelab.bigdata.spark.search.evaluation


import java.io.PrintWriter

import it.agilelab.bigdata.spark.search.dsl.MatchAnyQuery
import it.agilelab.bigdata.spark.search.impl.analyzers.EnglishWikipediaAnalyzer
import it.agilelab.bigdata.spark.search.impl.queries.DefaultQueryConstructor
import it.agilelab.bigdata.spark.search.impl.{LuceneConfig, PartitionsIndexLuceneRDD}
import it.agilelab.bigdata.spark.search.utils.wikipage
import org.elasticsearch.spark._

import sys.process._

/** Evaluates execution performance of Elasticsearch.
	*/
class ElasticsearchExecutionPerformanceEvaluator(log: PrintWriter) extends ExecutionPerformanceEvaluator(log) {
	override def name = this.getClass.getName
	override def desc: String = "Evaluates indexing performance using Wikipedia dump."
	
	// elasticsearch configs
	conf.set("es.index.auto.create", "true")
	
	override def run(): Unit = {
		log("Running execution performance tests")
		// get datasets
		val subsetsRDDs = for ((subsetName, subsetSize) <- subsets) yield {
			(subsetName, subsetSize, sc.objectFile[wikipage](Paths.enwikiSubsets + subsetName + "/"))
		}
		
		val luceneConfig = LuceneConfig(classOf[EnglishWikipediaAnalyzer], classOf[EnglishWikipediaAnalyzer], classOf[DefaultQueryConstructor])
			.setCompactIndex(true)
		
		// run tests
		for ((subsetName, subsetSize, rdd) <- subsetsRDDs) {
			log("Indexing " + subsetName)
			
			// make sure the subset is materialized and cached
			rdd.persist()
			rdd.count()
			
			val start = now
			
			rdd.saveToEs(subsetName, Map("es.nodes" -> "127.0.0.1:9200,127.0.0.1:9201,127.0.0.1:9300,127.0.0.1:9301"))
			
			val end = now
			log("Result: Indexing time: " + subsetName + "," + "elasticsearch" + "," + secs(start, end))
			
			//	log("Result: Indices info: " + subsetName + "," + indexType + "," +
			//		indicesInfo.numIndices + "," + indicesInfo.numDocuments + "," + indicesInfo.sizeBytes)
			
			log("Generating queries")
			val searchableRDD = PartitionsIndexLuceneRDD[wikipage](rdd, luceneConfig)
			val queryTests = generateQueries(searchableRDD.getTermCounts)
			log("Done")
				
			for ((queryType, queries) <- queryTests) {
				log("Running " + queryType + " queries")
				val start = now
				
				for (query <- queries) {
					val castedQuery = query.asInstanceOf[MatchAnyQuery]
					// create es uri query of the form "?q=field:(term1 term2 term3)&size=50"
					val queryString = f"?q=${castedQuery.field}:(${castedQuery.termSet.terms.mkString(" ")})&size=$maxHits"
					val result = sc.esRDD(subsetName, queryString)
					result.count()
				}
				
				val end = now
				log("Result: Query time: " + subsetName + "," + "elasticsearch" + "," + queryType + "," + secs(start, end))
			}
			
			// throw away index
			val deleteIndexCommand = s"curl -XDELETE 'http://localhost:9200/$subsetName'"
			deleteIndexCommand !
			
			// throw away subset
			rdd.unpersist(true)
		}
	}
}
