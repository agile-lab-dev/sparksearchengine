package it.agilelab.bigdata.spark.search.impl.queries

import it.agilelab.bigdata.spark.search.RawQuery
import it.agilelab.bigdata.spark.search.impl.LuceneConfig
import org.apache.lucene.util.QueryBuilder

/**
 * Builds standard queries using Lucene's QueryBuilder on the "text" field.
 */
class DefaultQueryConstructor(cfg: LuceneConfig) extends QueryConstructor(cfg) {
	val queryTimeAnalyzer = config.getQueryTimeAnalyzer
	val queryBuilder = new QueryBuilder(queryTimeAnalyzer)

	override def constructQuery(query: RawQuery) = {
		queryBuilder.createBooleanQuery("text", query.queryString)
	}
}