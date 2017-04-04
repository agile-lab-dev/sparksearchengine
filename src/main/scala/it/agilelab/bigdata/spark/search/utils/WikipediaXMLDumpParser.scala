package it.agilelab.bigdata.spark.search.utils

import java.io.InputStream

import scala.io.BufferedSource
import scala.xml.pull._

class WikipediaXMLDumpParser(xmlInputStream: InputStream) extends Iterable[(String, String)] {
	def iterator = new Iterator[(String, String)] {
		val xml = new XMLEventReader(new BufferedSource(xmlInputStream, 524288))
		
		def hasNext = xml.hasNext //TODO: proper check! currently it returns an extra empty page as the last one.
		def next = {
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
	}
}