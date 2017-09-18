package it.agilelab.bigdata.spark.search.utils

import java.io.ByteArrayInputStream

import com.databricks.spark.xml.XmlInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.BufferedSource
import scala.xml.pull._


object WikipediaXmlDumpParser {
	/**
		* Parse a compressed Wikipedia xml dump (wiki-date-pages-articles-multistream.xml.bz2) into an RDD[wikipage], and
		* save it as an object file.
		*
		* Takes two arguments: the first is the path to the dump file, the second is the directory where the RDD
		* resulting from the parsing will be saved.
		*/
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Parse Wikipedia dump file")
			.setMaster("local[4]")
		val sc = new SparkContext(conf)
		
		// paths
		val xmlPath = args(0) // input dump file path
		val outputPath = args(1) // output directory path
		
		// read xml dump into an rdd of wikipages
		val wikipages = xmlDumpToRdd(sc, xmlPath)
		
		// save as object file of wikipage
		wikipages.coalesce(50).saveAsObjectFile(outputPath)
	}
	
	/**
		* Parse a compressed Wikipedia xml dump (wiki-date-pages-articles-multistream.xml.bz2) into an RDD[wikipage].
		*
		* @param sc SparkContext to use
		* @param xmlPath input path to the compressed xml dump
		* @return an RDD of Wikipages
		*/
	def xmlDumpToRdd(sc: SparkContext, xmlPath: String): RDD[wikipage] = {
		// set hadoop input format
		sc.hadoopConfiguration.set(XmlInputFormat.START_TAG_KEY, "<page>")
		sc.hadoopConfiguration.set(XmlInputFormat.END_TAG_KEY, "</page>")
		sc.hadoopConfiguration.set(XmlInputFormat.ENCODING_KEY, "utf-8")
		
		// read the xml dump into records
		val records = sc.newAPIHadoopFile(
			xmlPath,
			classOf[XmlInputFormat],
			classOf[LongWritable],
			classOf[Text])
		
		// parses a record containing an xml <page> block into a (title: String, text: String)
		def parsePage(record: (LongWritable, Text)): (String, String) = {
			val xml = new XMLEventReader(new BufferedSource(new ByteArrayInputStream(record._2.getBytes)))
			
			var inPage = false
			var inTitle = false
			var inText = false
			var pageReady = false
			var title, text = ""
			while (xml.hasNext && !pageReady) {
				var event = xml.next()
				/* update "state machine" based on event */
				event match {
					case event: EvElemStart => {
						val castedEvent = event.asInstanceOf[EvElemStart]
						if (castedEvent.label.equalsIgnoreCase("page")) {
							inPage = true
						} else if (inPage && castedEvent.label.equalsIgnoreCase("title")) {
							inTitle = true
						} else if (inPage && castedEvent.label.equalsIgnoreCase("text")) {
							inText = true
						}
					}
					case event: EvText => {
						val castedEvent = event.asInstanceOf[EvText]
						if (inTitle) {
							title = castedEvent.text
						} else if (inText) {
							text = if (text == "") castedEvent.text else text + "\"" + castedEvent.text
						}
					}
					case event: EvElemEnd => {
						val castedEvent = event.asInstanceOf[EvElemEnd]
						if (castedEvent.label.equalsIgnoreCase("page")) {
							inPage = false
							pageReady = true
						} else if (inPage && castedEvent.label.equalsIgnoreCase("title")) {
							inTitle = false
						} else if (inPage && castedEvent.label.equalsIgnoreCase("text")) {
							inText = false
						}
					}
					case _ => ;
				}
			}
			(title, text)
		}
		
		// parse records into tuples
		val wikipedia = records map parsePage
		
		// map tuple into wikipage
		val wikipages = wikipedia map { case (title, text) => wikipage(title, text) }
		
		wikipages
	}
}
