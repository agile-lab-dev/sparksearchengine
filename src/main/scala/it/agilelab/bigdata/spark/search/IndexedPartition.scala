package it.agilelab.bigdata.spark.search

import it.agilelab.bigdata.spark.search.dsl.DslQuery

/**
	* An indexed partition.
	*
	*/
abstract class IndexedPartition[T] extends Serializable {
	private[search] def iterator: Iterator[T]

	def getElement(id: Long): Option[T]

	def search(query: Query, maxHits: Int): Iterator[(T, Double)]

	def search[V](query: Query, maxHits: Int, transformer: T => V): Iterator[(V, Double)]

	def lightSearch(query: Query, maxHits: Int): Iterator[(Long, Double)]

	def batchSearch(queries: Iterator[(Long, DslQuery)], maxHits: Int): Iterator[(Long, Iterator[(T, Double)])]

	def batchSearch[V](queries: Iterator[(Long, DslQuery)], maxHits: Int, transformer: T => V): Iterator[(Long, Iterator[(V, Double)])]

	def lightBatchSearch(queries: Iterator[(Long, DslQuery)], maxHits: Int): Iterator[(Long, Iterator[(Long, Double)])]

	def batchSearchRaw(queries: Iterator[(Long, RawQuery)], maxHits: Int): Iterator[(Long, Iterator[(T, Double)])]

	def batchSearchRaw[V](queries: Iterator[(Long, RawQuery)], maxHits: Int, transformer: T => V): Iterator[(Long, Iterator[(V, Double)])]

	def lightBatchSearchRaw(queries: Iterator[(Long, RawQuery)], maxHits: Int): Iterator[(Long, Iterator[(Long, Double)])]
}
