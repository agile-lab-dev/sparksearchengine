package it.agilelab.bigdata.spark.search.evaluation.utils

import java.io.File

import scala.io.Source
import scala.util.Random

import edu.cmu.lemurproject.{WarcFileInputFormat, WarcHTMLResponseRecord, WritableWarcRecord}
import it.agilelab.bigdata.spark.search.evaluation.Paths
import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.jsoup.Jsoup

/** Ingest the Clueweb12-B13 dataset.
	*/
object IngestClueWeb {
	def getCluewebpagesRDD(cluewebDirectoryPath: String, sc: SparkContext): RDD[cluewebpage] = {
		// get all the warc files
		val warcPathsCommaSeparated = listWarcFilesUsingList(cluewebDirectoryPath)
		
		// read the warc files into records
		val records = sc.hadoopFile(
			warcPathsCommaSeparated,
			classOf[WarcFileInputFormat],
			classOf[LongWritable],
			classOf[WritableWarcRecord])
		
		val htmlResponses = records.map(_._2.getRecord)
			.filter(_.getHeaderRecordType == "response")
			.map(record => new WarcHTMLResponseRecord(record))
		
		val cluewebpages = htmlResponses map {
			response =>
				val trecID = response.getTargetTrecID
				val rawContent = response.getRawRecord
					.getContentUTF8 // get content as utf8 string
				val content = getTextFromHTMLJsoup(rawContent)
				
				cluewebpage(trecID, content)
		}
	
		cluewebpages
	}
	
	// list from filesystem
	def listWarcFilesUsingFS(cluewebDirectoryPath: String): String = {
		val segments = (0 to 19).map(x => f"ClueWeb12_$x%02d").map(cluewebDirectoryPath + _ + File.separator)
		val directories = segments.flatMap(segment => new File(segment).listFiles().toList).map(_.getAbsolutePath)
		val warcPaths = directories.flatMap(directory => new File(directory).listFiles().toList).map(_.getAbsolutePath)
		val warcPathsCommaSeparated = warcPaths.mkString(",")
		
		warcPathsCommaSeparated
	}
	
	// use ready-made file list to avoid recursive directory listing ($$ on S3!)
	def listWarcFilesUsingList(cluewebDirectoryPath: String): String = {
		
		val warcFiles = Source.fromInputStream(this.getClass.getClassLoader.getResourceAsStream(Paths.cluewebFilesList)).getLines()
		val warcPaths = warcFiles.map(warcFile => cluewebDirectoryPath + warcFile)
		val warcPathsCommaSeparated = warcPaths.mkString(",")
		
		warcPathsCommaSeparated
	}
	
	// use ready-made file list to avoid recursive directory listing ($$ on S3!) and randomize their order
	def listWarcFilesUsingListRandomized(cluewebDirectoryPath: String): String = {
		
		val warcFiles = Source.fromInputStream(this.getClass.getClassLoader.getResourceAsStream(Paths.cluewebFilesList)).getLines()
		val warcPaths = warcFiles.map(warcFile => cluewebDirectoryPath + warcFile)
		val randomizedWarcPaths = new Random(42).shuffle(warcPaths)
		val warcPathsCommaSeparated = randomizedWarcPaths.mkString(",")
		
		warcPathsCommaSeparated
	}
	
	val scriptRegex = """<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>""".r
	val styleRegex = """<style\b[^<]*(?:(?!<\/style>)<[^<]*)*<\/style>""".r
	val tagRegex = """<[^>]*>""".r
	
	def getTextFromHTML(html: String): String = {
		try {
			val noScripts = scriptRegex.replaceAllIn(html, " ")
			val noStyle = styleRegex.replaceAllIn(noScripts, " ")
			val text = tagRegex.replaceAllIn(noStyle, " ")
			
			text
		} catch {
			case t: Throwable => ""
		}
	}
	
	def getTextFromHTMLJsoup(html: String): String = {
		try {
			val doc = Jsoup.parse(html)
			doc.text()
		} catch {
			case t: Throwable => ""
		}
	}
}

