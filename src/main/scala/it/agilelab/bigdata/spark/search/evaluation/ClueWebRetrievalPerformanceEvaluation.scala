package it.agilelab.bigdata.spark.search.evaluation

import java.io.PrintWriter
import java.io.InputStream

import it.agilelab.bigdata.spark.search.Storeable
import utils.IngestClueWeb
import it.agilelab.bigdata.spark.search.impl._
import it.agilelab.bigdata.spark.search.dsl._
import it.agilelab.bigdata.spark.search.impl.analyzers.EnglishAnalyzer
import it.agilelab.bigdata.spark.search.impl.queries.DefaultQueryConstructor
import it.agilelab.bigdata.spark.search.impl.similarities.ConfigurableBM25Similarity
import it.agilelab.bigdata.spark.search.utils.TrecTopicParser
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/** Evaluate retrieval performance using the ClueWeb12-B13 dataset and topics & qrels from TREC Web Tracks 2013/2014.
	*/
class ClueWebRetrievalPerformanceEvaluation(logFile: PrintWriter) extends Evaluator(logFile) {
	override def name: String = this.getClass.getName
	override def desc: String = "Evaluates retrieval performance using ClueWeb12-B13 and TREC Web Track 2013/1014 topics & qrels."
	
	
	// invoked once
	override def prepare(): Unit = {}
	
	def run(): Unit = {
		val clueweb = Datasets.ClueWeb12_B13
		
		log("Using dataset:")
		log(clueweb.toString)
		
		log("Ingesting clueweb")
		val cluewebRDD = IngestClueWeb.getCluewebpagesRDD(clueweb.path, sc)
		log("Done")
		
		log("Creating searchable")
		val config = LuceneConfig(classOf[EnglishAnalyzer], classOf[EnglishAnalyzer], classOf[DefaultQueryConstructor])
		  .setConfigurableSimilarityClass(classOf[ConfigurableBM25Similarity], ConfigurableBM25Similarity.makeConf(0.9d, 0.4d))
		val searchable = DistributedIndexLuceneRDD.fromStoreableWithGlobalIDF(cluewebRDD.asInstanceOf[RDD[Storeable[String]]], 35, config, "").persist(StorageLevel.MEMORY_AND_DISK_SER)
		log("Done")
		
		log("Loading queries")
		val queries2013 = loadQueries(this.getClass.getClassLoader.getResourceAsStream(Paths.trecWeb2013Topics))
		val queries2014 = loadQueries(this.getClass.getClassLoader.getResourceAsStream(Paths.trecWeb2014Topics))
		log("Done")
		
		log("Defining batch searches")
		val results2013 = searchable.batchSearch(queries2013, 1000, 1000)
		log("Done for 2013")
		val results2014 = searchable.batchSearch(queries2014, 1000, 1000)
		log("Done for 2014")
		
		log("Collecting results")
		val results2013Collected = results2013.collect()
		log("Done for 2013")
		val results2014Collected = results2014.collect()
		log("Done for 2014")
		
		log("Converting to TREC format")
		val results2013TrecFormat = formatResultsToTrec(results2013Collected.toList)
		val results2014TrecFormat = formatResultsToTrec(results2014Collected.toList)
		log("Done")
		
		log("Results for TREC Web 2013 topics:")
		results2013TrecFormat.foreach(log)
		log("Results for TREC Web 2014 topics:")
		results2014TrecFormat.foreach(log)
		log("Done")
		
		val indicesInfo = searchable.getIndicesInfo
		log(f"Indices info: numIndices: ${indicesInfo.numIndices}\tnumDocuments: ${indicesInfo.numDocuments}\tsizeBytes: ${indicesInfo.sizeBytes}")
		log("Detailed indices info:")
		indicesInfo.indicesInfo foreach {
			indexInfo =>
				log(f"\tnumDocuments: ${indexInfo.numDocuments}\tnumSegments: ${indexInfo.numSegments}\tsizeBytes: ${indexInfo.sizeBytes}\t")
		}
		
		readLine()
	}
	
	def loadQueries(data: InputStream): List[(Long, DslQuery)] = {
		val topics = new TrecTopicParser(data).iterator.toList
		
		// drop the last one, because the parser returns an extra empty one at the end
		val topicsNoLast = topics.take(topics.size-1)
		
		topicsNoLast map { case (trecId, query) => (trecId.toLong, "content" matchText query) }
	}
	
	def formatResultsToTrec(results: List[(Long, Array[(String, Double)])]): List[String] = {
		results flatMap {
		case (query, queryRresults) =>
			val resultsWithRank = queryRresults.toList.zipWithIndex
			resultsWithRank map {
				case ((docId, score), rank) =>
					f"$query Q0 $docId $rank $score sparksearch"
			}
		}
	}
}
