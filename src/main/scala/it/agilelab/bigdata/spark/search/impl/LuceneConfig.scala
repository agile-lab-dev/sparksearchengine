package it.agilelab.bigdata.spark.search.impl

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.typesafe.config.ConfigException.BadValue
import com.typesafe.config._
import it.agilelab.bigdata.spark.search.impl.analyzers.{ConfigurableAnalyzer, DefaultAnalyzer}
import it.agilelab.bigdata.spark.search.impl.queries.{DefaultQueryConstructor, QueryConstructor}
import it.agilelab.bigdata.spark.search.impl.similarities.ConfigurableSimilarity
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.search.similarities.{BM25Similarity, Similarity}


//FIXME: isAssignableFrom can give wrong results on scala classes
class LuceneConfig(private var config: Config) extends Serializable with KryoSerializable {
	import LuceneConfig._

	/* methods for config read/write */
	
	def getIndexTimeAnalyzerClass = {
		val analyzerClass = Class.forName(config.getString(IndexTimeAnalyzerClassProperty))
		if (checkAnalyzerClass(analyzerClass))
			analyzerClass
		else
			throw new BadValue(IndexTimeAnalyzerClassProperty, InvalidAnalyzerMessage)
	}

	def setIndexTimeAnalyzerClass(analyzerClass: Class[_]) = {
		if (checkAnalyzerClass(analyzerClass))
			new LuceneConfig(
				config.withValue(IndexTimeAnalyzerClassProperty,
				ConfigValueFactory.fromAnyRef(analyzerClass.getName)))
		else
				throw new IllegalArgumentException(InvalidAnalyzerMessage)
	}

	def getQueryTimeAnalyzerClass = {
		val analyzerClass = Class.forName(config.getString(QueryTimeAnalyzerClassProperty))
		if (checkAnalyzerClass(analyzerClass))
			analyzerClass
		else
			throw new BadValue(QueryTimeAnalyzerClassProperty, InvalidAnalyzerMessage)
	}

	def setQueryTimeAnalyzerClass(analyzerClass: Class[_]) = {
		if (checkAnalyzerClass(analyzerClass))
			new LuceneConfig(
				config.withValue(QueryTimeAnalyzerClassProperty,
				ConfigValueFactory.fromAnyRef(analyzerClass.getName)))
		else
			throw new IllegalArgumentException(InvalidAnalyzerMessage)
	}

	def getQueryConstructorClass = {
		val queryConstructorClass = Class.forName(config.getString(QueryConstructorClassProperty))
		if (checkQueryConstructorClass(queryConstructorClass))
			queryConstructorClass
		else
			throw new IllegalArgumentException(InvalidQueryConstructorMessage)
	}

	def setQueryConstructorClass(queryConstructorClass: Class[_]) = {
		if (checkQueryConstructorClass(queryConstructorClass))
			new LuceneConfig(
				config.withValue(QueryConstructorClassProperty,
				ConfigValueFactory.fromAnyRef(queryConstructorClass.getName)))
		else
			throw new BadValue(QueryConstructorClassProperty, InvalidQueryConstructorMessage)
	}
	
	def getSimilarityClass = {
		val similarityClass = Class.forName(config.getString(SimilarityClassProperty))
		if (checkSimilarityClass(similarityClass))
			similarityClass
		else
			throw new IllegalArgumentException(InvalidSimilarityMessage)
	}
	
	def setSimilarityClass(similarityClass: Class[_]) = {
		if (checkSimilarityClass(similarityClass))
			new LuceneConfig(
				config.withValue(SimilarityClassProperty,
					ConfigValueFactory.fromAnyRef(similarityClass.getName)))
		else
			throw new BadValue(SimilarityClassProperty, InvalidSimilarityMessage)
	}
	
	def setConfigurableSimilarityClass(similarityClass: Class[_], similarityConfig: Config) = {
		if (checkSimilarityClass(similarityClass)) {
			val newConfig = config
				.withValue(SimilarityClassProperty, ConfigValueFactory.fromAnyRef(similarityClass.getName))
				.withValue(SimilarityConfProperty, ConfigValueFactory.fromMap(similarityConfig.root().unwrapped()))
			new LuceneConfig(newConfig)
		} else
			throw new BadValue(SimilarityClassProperty, InvalidSimilarityMessage)
	}
	
	def getConfigurableSimilarityConfig: Config = config.atPath(SimilarityConfProperty)
	
	def getCompactIndex: Boolean = {
		config.getBoolean(CompactIndexProperty)
	}
	
	def setCompactIndex(value: Boolean) = {
		new LuceneConfig(config.withValue(CompactIndexProperty, ConfigValueFactory.fromAnyRef(value)))
	}

	/* methods to instantiate analyzers, query constructors and similaritites */

	def getIndexTimeAnalyzer: Analyzer = instantiateAnalyzer(getIndexTimeAnalyzerClass)

	def getQueryTimeAnalyzer: Analyzer = instantiateAnalyzer(getQueryTimeAnalyzerClass)

	private def instantiateAnalyzer(analyzerClass: Class[_]) = analyzerClass match {
		case configurableAn if classOf[Configurable].isAssignableFrom(analyzerClass) => // configurable: instantiate with constructor with config
			configurableAn.getConstructor(classOf[LuceneConfig])
				.newInstance(this)
				.asInstanceOf[ConfigurableAnalyzer]
				.getAnalyzer
		case luceneAn if classOf[Analyzer].isAssignableFrom(analyzerClass) => // standard: instantiate
			luceneAn.newInstance()
				.asInstanceOf[Analyzer]
	}

	def getQueryConstructor: QueryConstructor = {
		getQueryConstructorClass.getConstructor(classOf[LuceneConfig])
		                        .newInstance(this)
		                        .asInstanceOf[QueryConstructor]
	}
	
	def getSimilarity: Similarity = {
		val similarityClass = getSimilarityClass
		similarityClass match {
			case configurableSim if classOf[Configurable].isAssignableFrom(similarityClass) => // configurable: instantiate with constructor with config
				configurableSim.getConstructor(classOf[LuceneConfig])
					.newInstance(this)
					.asInstanceOf[ConfigurableSimilarity]
					.getSimilarity
			case luceneSim if classOf[Similarity].isAssignableFrom(similarityClass) => // standard: instantiate
				getSimilarityClass.getConstructor()
					.newInstance()
					.asInstanceOf[Similarity]
		}
	}

	/**
		* Serialize this object using kryo.
		*/
	override def write(kryo: Kryo, output: Output): Unit = {
		// write fields
		val configString = config.root.render(ConfigRenderOptions.defaults())
		output.writeString(configString)
		output.flush()
	}

	/**
		* Deserialize this object using kryo.
		*/
	override def read(kryo: Kryo, input: Input): Unit = {
		// read fields
		val configString = input.readString()
		config = ConfigFactory.parseString(configString)
	}

	def getInfo: String = {
		config.root.render
	}
}

object LuceneConfig {
	val IndexTimeAnalyzerClassProperty = "indexTimeAnalyzerClass"
	val QueryTimeAnalyzerClassProperty = "queryTimeAnalyzerClass"
	val QueryConstructorClassProperty = "queryConstructorClass"
	val SimilarityClassProperty = "similarityClass"
	
	val SimilarityConfProperty = "similarityConf"
	
	val CompactIndexProperty = "compactIndex"

	val DefaultAnalyzerClass = classOf[DefaultAnalyzer]
	val DefaultQueryConstructorClass = classOf[DefaultQueryConstructor]
	val DefaultSimilarityClass = classOf[BM25Similarity]

	val InvalidAnalyzerMessage = "The analyzer must be a subtype of Lucene Analyzer or ConfigurableAnalyzer"
	val InvalidQueryConstructorMessage = "The query constructor must be a subtype of QueryConstructor"
	val InvalidSimilarityMessage = "The similarity must be a subtype of Lucene Similarity"

	// empty config
	def apply(): LuceneConfig = {
		new LuceneConfig(ConfigFactory.empty())
	}

	// apply with class parameters, subclasses of Analyzer and QueryBuilder
	def apply(
       indexTimeAnalyzerClass: Class[_],
       queryTimeAnalyzerClass: Class[_],
       queryConstructorClass: Class[_]): LuceneConfig = {
		defaultConfig()
			.setIndexTimeAnalyzerClass(indexTimeAnalyzerClass)
			.setQueryTimeAnalyzerClass(queryTimeAnalyzerClass)
			.setQueryConstructorClass(queryConstructorClass)
	}

	// default configuration
	def defaultConfig(): LuceneConfig = {
		new LuceneConfig(ConfigFactory.empty())
			.setIndexTimeAnalyzerClass(DefaultAnalyzerClass)
			.setQueryTimeAnalyzerClass(DefaultAnalyzerClass)
			.setQueryConstructorClass(DefaultQueryConstructorClass)
			.setSimilarityClass(DefaultSimilarityClass)
		  .setCompactIndex(false)
	}

	// loads the config in the standard Typesafe Config way
	def loadFromEnv(): LuceneConfig = {
		new LuceneConfig(ConfigFactory.load())
	}

	private def checkAnalyzerClass(analyzerClass: Class[_]): Boolean = {
		classOf[Analyzer].isAssignableFrom(analyzerClass) ||
			classOf[ConfigurableAnalyzer].isAssignableFrom(analyzerClass)
	}

	private def checkQueryConstructorClass(queryConstructorClass: Class[_]): Boolean = {
		classOf[QueryConstructor].isAssignableFrom(queryConstructorClass)
	}
	
	private def checkSimilarityClass(similarityClass: Class[_]): Boolean = {
		classOf[Similarity].isAssignableFrom(similarityClass) ||
			classOf[ConfigurableSimilarity].isAssignableFrom(similarityClass)
	}
}