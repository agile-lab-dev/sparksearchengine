package it.agilelab.bigdata.spark.search

import it.agilelab.bigdata.spark.search.dsl.DslQuery

/**
	* An index.
	*/
abstract class Index extends Serializable {
	def searchAny(query: Query, maxHits: Int): Iterator[(Long, Double)]

	def search(query: DslQuery, maxHits: Int): Iterator[(Long, Double)]

	def search(query: RawQuery, maxHits: Int): Iterator[(Long, Double)]

	def batchSearch(queries: Iterable[(Long, DslQuery)], maxHits: Int): Iterator[(Long, Iterator[(Long, Double)])]

	def batchSearchRaw(queries: Iterator[(Long, RawQuery)], maxHits: Int): Iterator[(Long, Iterator[(Long, Double)])]
	
	def getDocumentCounts: Map[String, Long]
	
	def getTermCounts: Map[String, Map[String, Long]]
	
	def getTermIDFs: Map[String, Map[String, Float]]
	
	def getIndexInfo: IndexInfo
}