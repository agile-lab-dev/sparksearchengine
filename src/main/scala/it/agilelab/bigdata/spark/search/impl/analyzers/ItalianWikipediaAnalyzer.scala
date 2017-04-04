package it.agilelab.bigdata.spark.search.impl.analyzers

import it.agilelab.bigdata.spark.search.impl.LuceneConfig

/**
	* Configurable analyzer for Italian which is aware of Wikipedia syntax.
	*
	* @author Nicolò Bidotti
	*/
class ItalianWikipediaAnalyzer(cfg: LuceneConfig) extends ConfigurableAnalyzer(cfg) {
	val analyzer = new LuceneItalianWikipediaAnalyzer()
	override def getAnalyzer = analyzer
}