package it.agilelab.bigdata.spark.search.impl.analyzers

import it.agilelab.bigdata.spark.search.impl.LuceneConfig
import org.apache.lucene.analysis.it.{ItalianAnalyzer => LuceneItalianAnalyzer}

class ItalianAnalyzer(cfg: LuceneConfig) extends ConfigurableAnalyzer(cfg) {
	val analyzer = new LuceneItalianAnalyzer()
	override def getAnalyzer = analyzer
}