package it.agilelab.bigdata.spark.search.evaluation

import java.io.PrintWriter

import it.agilelab.bigdata.spark.search.dsl._
import it.agilelab.bigdata.spark.search.SearchableRDD
import it.agilelab.bigdata.spark.search.impl._
import it.agilelab.bigdata.spark.search.impl.analyzers.EnglishWikipediaAnalyzer
import it.agilelab.bigdata.spark.search.impl.queries.DefaultQueryConstructor
import it.agilelab.bigdata.spark.search.utils.wikipage
import org.apache.spark.storage.StorageLevel

/** Evaluates execution performance of Sparksearch
	*/
class SparksearchExecutionPerformanceEvaluator(log: PrintWriter) extends ExecutionPerformanceEvaluator(log) {
	override def name = this.getClass.getName
	override def desc: String = "Evaluates indexing performance using Wikipedia dump."
	
	override def run(): Unit = {
		log("Running execution performance tests")
		// get datasets
		val subsetsRDDs = for ((subsetName, subsetSize) <- subsets) yield {
			(subsetName, subsetSize, sc.objectFile[wikipage](Paths.enwikiSubsets + subsetName + "/"))
		}
		
		// test parameters
		object Index {
			val broadcast = "broadcastIndex"
			val partitions = "partitionsIndex"
			val distributed = "distributedIndex"
		}
		val indexTests = List[(String, Int, Int, Int)]( // index type, number of partitions (input), max subset size
			(Index.broadcast, 0, 0, 1000001),
			(Index.partitions, 0, 0, 5000001),
			(Index.distributed, 0, 20, 20000000),
			(Index.distributed, 0, 10, 20000000)
		)
		
		val luceneConfig = LuceneConfig(classOf[EnglishWikipediaAnalyzer], classOf[EnglishWikipediaAnalyzer], classOf[DefaultQueryConstructor])
			.setCompactIndex(true)
		
		// run tests
		for ((subsetName, subsetSize, rdd) <- subsetsRDDs; (indexType, partitions, indices, maxSubsetSize) <- indexTests) {
			log("Indexing " + subsetName)
			
			// make sure the subset is materialized and cached
			rdd.persist()
			rdd.count()
			
			if (subsetSize > maxSubsetSize) {
				log("Result: Indexing time: " + subsetName + "," + indexType + "," + Double.NaN)
			} else {
				val start = now
				
				val searchableRDD: SearchableRDD[wikipage] = indexType match {
					case Index.broadcast =>   BroadcastIndexLuceneRDD[wikipage](rdd, luceneConfig)
					case Index.partitions =>  PartitionsIndexLuceneRDD[wikipage](rdd, luceneConfig)
					case Index.distributed => DistributedIndexLuceneRDD[wikipage](rdd, indices, luceneConfig)
				}
				searchableRDD.persist(StorageLevel.MEMORY_AND_DISK)
				
				searchableRDD.search("title" matchText "italy", 10).collect() // materialize & index
				
				val end = now
				log("Result: Indexing time: " + subsetName + "," + indexType + "," + secs(start, end))
				
				val indicesInfo = searchableRDD.getIndicesInfo
				log("Result: Indices info: " + subsetName + "," + indexType + "," +
					indicesInfo.numIndices + "," + indicesInfo.numDocuments + "," + indicesInfo.sizeBytes)
				log("Detailed indices info:")
				indicesInfo.indicesInfo foreach {
					indexInfo =>
						log(f"\tnumDocuments: ${indexInfo.numDocuments}\tnumSegments: ${indexInfo.numSegments}\tsizeBytes: ${indexInfo.sizeBytes}\t")
				}
				
				log("Generating queries")
				val queryTests = generateQueries(searchableRDD.getTermCounts)
				log("Done")
				
				for ((queryType, queries) <- queryTests) {
					log("Running " + queryType + " queries")
					val start = now
					
					val queriesIterable = queries.zipWithIndex.map(x => (x._2.toLong, x._1))
					val result = searchableRDD.batchSearch(queriesIterable, maxHits)
					result.count()
					
					val end = now
					log("Result: Query time: " + subsetName + "," + indexType + "," + queryType + "," + secs(start, end))
				}
				
				// throw away searchable
				searchableRDD.unpersist(true)
			}
			
			// throw away subset
			rdd.unpersist(true)
		}
	}
}
