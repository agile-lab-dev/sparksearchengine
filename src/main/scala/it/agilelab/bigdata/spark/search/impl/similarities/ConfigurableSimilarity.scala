package it.agilelab.bigdata.spark.search.impl.similarities

import it.agilelab.bigdata.spark.search.impl.{Configurable, LuceneConfig}
import org.apache.lucene.search.similarities.Similarity

abstract class ConfigurableSimilarity(conf: LuceneConfig) extends Configurable(conf) {
	override def getConfig: LuceneConfig = conf
	def getSimilarity: Similarity
}
