package it.agilelab.bigdata.spark.search.evaluation

import java.io.{InputStream, PrintWriter}

import it.agilelab.bigdata.spark.search.Storeable
import it.agilelab.bigdata.spark.search.dsl._
import it.agilelab.bigdata.spark.search.evaluation.utils.{IngestClueWeb, cluewebpage}
import it.agilelab.bigdata.spark.search.impl._
import it.agilelab.bigdata.spark.search.utils.TrecTopicParser
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/** Evaluate the size of the text in the ClueWeb12-B13 dataset, after extracting it from the HTML.
	* Returns the total number of pages, words and total text size.
	* The number of bytes for each string is calculated as String.getBytes().length.
	*/
class ClueWebSizeEvaluation(logFile: PrintWriter) extends Evaluator(logFile) {
	override def name: String = this.getClass.getName
	override def desc: String = "Evaluates the size of the ClueWeb12-B13 dataset."
	
	// invoked once
	override def prepare(): Unit = {}
	
	def run(): Unit = {
		val clueweb = Datasets.ClueWeb12_B13
		
		log("Using dataset:")
		log(clueweb.toString)
		
		log("Ingesting clueweb")
		val cluewebRDD = IngestClueWeb.getCluewebpagesRDD(clueweb.path, sc)
		log("Done")
		
		log("Calculating dataset size")
		val zero = (0l, 0l, 0l) // pages, words, bytes
		val seqOp = (counters: (Long, Long, Long), cwpage: cluewebpage) => {
			val pages = counters._1 + 1
			val words = counters._2 + cwpage.content.split(' ').length
			val bytes = counters._3 + cwpage.content.getBytes.length
			
			(pages, words, bytes)
		}
		val combOp = (counters1: (Long, Long, Long), counters2: (Long, Long, Long)) => {
			(counters1._1 + counters2._1, counters1._2 + counters2._2, counters1._3 + counters2._3)
		}
		val sizes = cluewebRDD.treeAggregate(zero)(seqOp, combOp)
		log("Done calculating dataset size")
		
		log("Total pages, words and bytes of cleaned text (as in, string.getBytes.length): " + sizes)
		
		readLine()
	}
}
