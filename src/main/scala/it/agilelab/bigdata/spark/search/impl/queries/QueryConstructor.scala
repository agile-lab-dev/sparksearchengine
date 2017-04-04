package it.agilelab.bigdata.spark.search.impl.queries

import it.agilelab.bigdata.spark.search.RawQuery
import it.agilelab.bigdata.spark.search.impl.{Configurable, LuceneConfig}
import org.apache.lucene.search.Query

abstract class QueryConstructor(cfg: LuceneConfig) extends Configurable(cfg) {
	def constructQuery(query: RawQuery): Query
	override def getConfig: LuceneConfig = config
}