package it.agilelab.bigdata.spark.search.examples

import scala.io.BufferedSource
import scala.xml.pull._
import java.io.ByteArrayInputStream

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.io.{LongWritable, Text}
import com.databricks.spark.xml.XmlInputFormat
import it.agilelab.bigdata.spark.search.evaluation.utils.wikipage


/**
	* Parse a Wikipedia dump wiki-date-pages-articles-multistream.xml.bz2 int an RDD[wikpage], and save it as object file.
	*
	* Main method takes two arguments: the first is the path to the dump file, the second is the directory where the RDD
	* resulting from the parsing will be saved.
	*/
object ParseWikipediaDump {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Parse Wikipedia dump file")
		val sc = SparkContext.getOrCreate(conf)
		
		// paths
		val xmlPath = args(0) // input dump file path
		val outputPath = args(1) // output directory path
		
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
		val wikipedia = records map { parsePage(_) }
		
		// map tuple into wikipage
		val wikipages = wikipedia map { case (title, text) => wikipage(title, text) }
		
		// save as object file of wikipage
		wikipages.coalesce(50).saveAsObjectFile(outputPath)
	}
}
