package it.agilelab.bigdata.spark.search.impl.analyzers

import it.agilelab.bigdata.spark.search.impl.LuceneConfig
import org.apache.lucene.analysis.core.SimpleAnalyzer

class DefaultAnalyzer(cfg: LuceneConfig) extends ConfigurableAnalyzer(cfg) {
	val analyzer = new SimpleAnalyzer()
	override def getAnalyzer = analyzer
}