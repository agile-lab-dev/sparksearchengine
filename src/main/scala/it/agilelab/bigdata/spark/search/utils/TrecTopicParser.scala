package it.agilelab.bigdata.spark.search.utils

import java.io.InputStream

import scala.io.BufferedSource
import scala.xml.pull.{EvElemEnd, EvElemStart, EvText, XMLEventReader}


class TrecTopicParser(xmlInputStream: InputStream) extends Iterable[(Int, String)] {
	def iterator = new Iterator[(Int, String)] {
		val xml = new XMLEventReader(new BufferedSource(xmlInputStream, 524288))
		
		def hasNext = xml.hasNext //TODO: proper check! currently it returns an extra empty page as the last one.
		def next = {
			var inTopic = false
			var inQuery = false
			var topicReady = false
			var topicId = 0
			var query = ""
			while (xml.hasNext && !topicReady) {
				var event = xml.next()
				/* update "state machine" based on event */
				event match {
					case event: EvElemStart => {
						val castedEvent = event.asInstanceOf[EvElemStart]
						if (castedEvent.label.equalsIgnoreCase("topic")) {
							inTopic = true
							topicId = event.attrs("number").head.text.toInt
						} else if (inTopic && castedEvent.label.equalsIgnoreCase("query")) {
							inQuery = true
						}
					}
					case event: EvText => {
						val castedEvent = event.asInstanceOf[EvText]
						if (inQuery) {
							query = castedEvent.text
						}
					}
					case event: EvElemEnd => {
						val castedEvent = event.asInstanceOf[EvElemEnd]
						if (castedEvent.label.equalsIgnoreCase("topic")) {
							inTopic = false
							topicReady = true
						} else if (inTopic && castedEvent.label.equalsIgnoreCase("query")) {
							inQuery = false
						}
					}
					case _ => ;
				}
			}
			(topicId, query)
		}
	}
}
