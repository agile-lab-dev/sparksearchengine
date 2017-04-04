package it.agilelab.bigdata.spark.search.evaluation.utils

import java.io.ByteArrayInputStream

import scala.io.{BufferedSource, Codec}
import scala.xml.pull.{EvElemEnd, EvElemStart, EvText, XMLEventReader}

import com.databricks.spark.xml.XmlInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


object IngestWikipedia {
	def getWikipagesRDD(wikipediaDumpPath: String, sc: SparkContext): RDD[wikipage] = {
		
		// set hadoop input format
		sc.hadoopConfiguration.set(XmlInputFormat.START_TAG_KEY, "<page>")
		sc.hadoopConfiguration.set(XmlInputFormat.END_TAG_KEY, "</page>")
		sc.hadoopConfiguration.set(XmlInputFormat.ENCODING_KEY, "utf-8")
		
		// read the xml dump into records
		val records = sc.newAPIHadoopFile(
			wikipediaDumpPath,
			classOf[XmlInputFormat],
			classOf[LongWritable],
			classOf[Text])
		
		// parses a record containing an xml <page> block into a (title: String, text: String)
		def parsePage(record: (LongWritable, Text)): (String, String) = {
			val xml = new XMLEventReader(new BufferedSource(new ByteArrayInputStream(record._2.getBytes))(Codec.UTF8))
			
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
		val wikipedia = records map {
			parsePage
		}
		
		// map tuple into wikipage
		val wikipages = wikipedia map { case (title, text) => wikipage(title, text) }
		
		wikipages
	}
}
