package it.agilelab.bigdata.spark.search.impl.analyzers

import it.agilelab.bigdata.spark.search.impl.{Configurable, LuceneConfig}
import org.apache.lucene.analysis.Analyzer

abstract class ConfigurableAnalyzer(cfg: LuceneConfig) extends Configurable(cfg) {
	def getAnalyzer: Analyzer
	override def getConfig: LuceneConfig = config
}