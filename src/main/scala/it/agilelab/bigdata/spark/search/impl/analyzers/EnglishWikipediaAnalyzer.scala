package it.agilelab.bigdata.spark.search.impl.analyzers

import it.agilelab.bigdata.spark.search.impl.LuceneConfig

/**
	* Configurable analyzer for English which is aware of Wikipedia syntax.
	*
	* @author Nicolò Bidotti
	*/
class EnglishWikipediaAnalyzer(cfg: LuceneConfig) extends ConfigurableAnalyzer(cfg) {
	val analyzer = new LuceneEnglishWikipediaAnalyzer()
	override def getAnalyzer = analyzer
}