package it.agilelab.bigdata.spark.search.impl.analyzers

import it.agilelab.bigdata.spark.search.impl.LuceneConfig
import org.apache.lucene.analysis.en.{EnglishAnalyzer => LuceneEnglishAnalyzer}

class EnglishAnalyzer(cfg: LuceneConfig) extends ConfigurableAnalyzer(cfg) {
	val analyzer = new LuceneEnglishAnalyzer()
	override def getAnalyzer = analyzer
}
