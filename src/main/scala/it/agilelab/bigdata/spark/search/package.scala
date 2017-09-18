package it.agilelab.bigdata.spark


/** Core functionality of Spark Search resides here.
	*
	* [[it.agilelab.bigdata.spark.search.SearchableRDD]] and its implementations are the main classes which provide
	* user-facing funcitonality.
	*
	* There are three implementations available: [[it.agilelab.bigdata.spark.search.impl.BroadcastIndexLuceneRDD]],
	* [[it.agilelab.bigdata.spark.search.impl.PartitionsIndexLuceneRDD]] and
	* [[it.agilelab.bigdata.spark.search.impl.DistributedIndexLuceneRDD]].
	*/
package object search {
	val SPARK_SEARCH_VERSION = "0.1"
}
