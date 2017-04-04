package it.agilelab.bigdata.spark.search.impl.similarities

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import it.agilelab.bigdata.spark.search.impl.LuceneConfig
import org.apache.lucene.search.similarities.{BM25Similarity, Similarity}

class ConfigurableBM25Similarity(conf: LuceneConfig) extends ConfigurableSimilarity(conf) {
	import ConfigurableBM25Similarity._
	
	override def getSimilarity: Similarity = {
		val similarityConf = conf.getConfigurableSimilarityConfig
		val k1 = if (similarityConf.hasPath(K1Subproperty)) similarityConf.getDouble(K1Subproperty) else DefaultK1
		val b = if (similarityConf.hasPath(BSubproperty)) similarityConf.getDouble(BSubproperty) else DefaultB
		
		new BM25Similarity(k1.toFloat, b.toFloat)
	}
}
object ConfigurableBM25Similarity {
	val K1Subproperty = "k1"
	val BSubproperty = "b"
	
	val DefaultK1 = 1.2d
	val DefaultB = 0.75d
	
	def makeConf(k1: Double, b: Double): Config = {
		ConfigFactory.empty()
			.withValue(K1Subproperty, ConfigValueFactory.fromAnyRef(k1))
			.withValue(BSubproperty, ConfigValueFactory.fromAnyRef(b))
	}
}
