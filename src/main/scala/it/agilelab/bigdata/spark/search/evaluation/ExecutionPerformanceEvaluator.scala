package it.agilelab.bigdata.spark.search.evaluation

import java.io.{File, PrintWriter}

import scala.util.Random

import it.agilelab.bigdata.spark.search.dsl._
import it.agilelab.bigdata.spark.search.evaluation.utils.{IngestWikipedia, wikipage}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


abstract class ExecutionPerformanceEvaluator(logFile: PrintWriter) extends Evaluator(logFile) {
	override def name = this.getClass.getName
	override def desc: String = "Evaluates indexing performance using Wikipedia dump."
	
	override def prepare(): Unit = {
		val enwiki = Datasets.enwiki
		
		log("Using dataset:")
		log(enwiki.toString)
		
		log("Reading dataset")
		val wiki: RDD[wikipage] = IngestWikipedia.getWikipagesRDD(enwiki.path, sc).persist(StorageLevel.MEMORY_AND_DISK)
		
		log("Creating subsets")
		val seed = 42 // make it reproducible
		for ((subsetName, subsetSize) <- subsets) {
			// if we're using a local fs, check if the subset already exists before creating it
			val subsetExists = if (Paths.enwikiSubsets.startsWith("/")) {
				val successFile = new File(Paths.enwikiSubsets + subsetName + "/_SUCCESS")
				successFile.exists()
			} else false
			
			if (!subsetExists) {
				val samplingFactor = subsetSize * singlePageFactor
				log("Creating subset " + subsetName + " of size " + subsetSize + " (" + samplingFactor + " sampling factor)")
				val subset = wiki.sample(withReplacement = false, samplingFactor, seed)
				val newPartitionNumber = Math.ceil(wiki.getNumPartitions * (subsetSize.toDouble / numPages)).toInt
				subset.coalesce(newPartitionNumber, shuffle = false).saveAsObjectFile(Paths.enwikiSubsets + subsetName + "/")
				log("Done creating subset " + subsetName)
			}
		}
		log("Done creating subsets")
		
		wiki.unpersist()
	}
	
	val maxHits = 50
	
	val numPages = 17069180 // enwiki has 17069180 pages
	val singlePageFactor = 1d / numPages
	val subsets: Map[String, Int] = Map(
		"1million"    ->   1000000,
		"5million"    ->   5000000,
		"10million"   ->  10000000
	)
	
	def generateQueries(termsInfo: Map[String, Map[String, Long]]): List[(String, Array[DslQuery])] = {
		// get most common/rare terms
		val termsSorted = termsInfo("text") // get text field
		  .toList
		  .sortBy(_._2)
		  .map(_._1)
		val numTerms = termsSorted.size
		val topTerms = termsSorted.slice(numTerms - 1000, numTerms).toArray
		val bottomTerms = termsSorted.slice(0, 1000).toArray
		
		// generate queries
		val rng = new Random(42) // make it reproducible
		val threeCommonTermsQueries = generateRandomMatchAnyQueries(10000, 3, topTerms, rng).toArray
		val fiveCommonTermsQueries = generateRandomMatchAnyQueries(10000, 5, topTerms, rng).toArray
		val threeRareTermsQueries = generateRandomMatchAnyQueries(10000, 3, bottomTerms, rng).toArray
		val fiveRareTermsQueries = generateRandomMatchAnyQueries(10000, 5, bottomTerms, rng).toArray
		
		// prepare query tests
		val queryTests = List[(String, Array[DslQuery])]( // query type, queries
			("3CommonTerms100",  threeCommonTermsQueries.take(100)),
			("3CommonTerms250",  threeCommonTermsQueries.take(250)),
			("3CommonTerms500",  threeCommonTermsQueries.take(500)),
			("3CommonTerms1000", threeCommonTermsQueries.take(1000)),
			("3CommonTerms2500", threeCommonTermsQueries.take(2500)),
			("5CommonTerms100",  fiveCommonTermsQueries.take(100)),
			("5CommonTerms250",  fiveCommonTermsQueries.take(250)),
			("5CommonTerms500",  fiveCommonTermsQueries.take(500)),
			("5CommonTerms1000", fiveCommonTermsQueries.take(1000)),
			("5CommonTerms2500", fiveCommonTermsQueries.take(2500)),
			("3RareTerms100",    threeRareTermsQueries.take(100)),
			("3RareTerms250",    threeRareTermsQueries.take(250)),
			("3RareTerms500",    threeRareTermsQueries.take(500)),
			("3RareTerms1000",   threeRareTermsQueries.take(1000)),
			("3RareTerms2500",   threeRareTermsQueries.take(2500)),
			("5RareTerms100",    fiveRareTermsQueries.take(100)),
			("5RareTerms250",    fiveRareTermsQueries.take(250)),
			("5RareTerms500",    fiveRareTermsQueries.take(500)),
			("5RareTerms1000",   fiveRareTermsQueries.take(1000)),
			("5RareTerms2500",   fiveRareTermsQueries.take(2500))
		)
		
		queryTests
	}
	
	def generateRandomMatchAnyQueries(numQueries: Int, numTermsPerQuery: Int, terms: Array[String], rng: Random): Iterator[DslQuery] = {
		val numTerms = terms.length
		(1 to numQueries*numTermsPerQuery) // get numTerms terms for each of numQueries queries
			.map(_ => rng.nextInt(numTerms)) // get random indices
			.map(terms) // get corresponding top terms
			.grouped(numTermsPerQuery) // group terms for each query
			.map(terms => "text" matchAny termSet(terms:_*)) // create queries
	}
}
