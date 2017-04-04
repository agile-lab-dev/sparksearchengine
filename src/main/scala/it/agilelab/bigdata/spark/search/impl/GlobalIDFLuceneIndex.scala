package it.agilelab.bigdata.spark.search.impl

import java.io.{IOException, ObjectInputStream}

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import it.agilelab.bigdata.spark.search._
import it.agilelab.bigdata.spark.search.dsl.{DslQuery, NegatedQuery}
import it.agilelab.bigdata.spark.search.impl.queries.QueryConstructor
import it.agilelab.bigdata.spark.search.impl.similarities.BM25WithGlobalIDFSimilarity
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.IndexSearcher
import org.apache.spark.broadcast.Broadcast

/**
  * Lucene-based implementation of {@link it.agilelab.bigdata.spark.search.Index} with global IDF support.
  */
@SerialVersionUID(1l)
class GlobalIDFLuceneIndex private[impl](
		private val directory_ : BigChunksRAMDirectory,
		private val config_ : LuceneConfig,
		private[impl] var globalIdf: Broadcast[Map[String, Map[String, Float]]],
		@transient private var indexReader: DirectoryReader,
		@transient private var indexSearcher: IndexSearcher,
		@transient private var indexTimeAnalyzer: Analyzer,
		@transient private var queryTimeAnalyzer: Analyzer,
		@transient private var queryConstructor: QueryConstructor)
	extends LuceneIndex(directory_,
                      config_,
                      indexReader,
                      indexSearcher,
                      indexTimeAnalyzer,
                      queryTimeAnalyzer,
                      queryConstructor) {
	
	/**
	  * Deserialize this object using standard Java serialization.
	  */
	@throws(classOf[IOException])
	@throws(classOf[ClassNotFoundException])
	private def readObject(ois: ObjectInputStream): Unit = {
		// read non-transient fields
		// call defaultReadObject because the spec says so
		ois.defaultReadObject()
		// rebuild transient fields
		this.rebuildTransientFields()
	}

	/**
		* Serialize this object using kryo.
		*/
	override def write(kryo: Kryo, output: Output): Unit = {
		// write non-transient fields
		kryo.writeObject(output, directory)
		output.flush()
		kryo.writeObject(output, config)
		output.flush()
		kryo.writeObject(output, globalIdf)
		output.flush()
	}

	/**
		* Deserialize this object using kryo.
		*/
	override def read(kryo: Kryo, input: Input): Unit = {
		// read non-transient fields
		directory = kryo.readObject(input, classOf[BigChunksRAMDirectory])
		config = kryo.readObject(input, classOf[LuceneConfig])
		globalIdf = kryo.readObject(input, classOf[Broadcast[Map[String, Map[String, Float]]]])
		// rebuild transient fields
		this.rebuildTransientFields()
	}

	private def rebuildTransientFields(): Unit = {
		// create Lucene IndexReader, IndexSearcher and analyzers
		indexReader = DirectoryReader.open(directory)
		val similarity = new BM25WithGlobalIDFSimilarity(globalIdf.value)
		indexSearcher = new IndexSearcher(indexReader)
		indexSearcher.setSimilarity(similarity)
		indexTimeAnalyzer = config.getIndexTimeAnalyzer
		queryTimeAnalyzer = config.getQueryTimeAnalyzer
		queryConstructor = config.getQueryConstructor
	}
}

object GlobalIDFLuceneIndex {
	private[search] def apply[T](directory: BigChunksRAMDirectory,
		                           config: LuceneConfig,
		                           globalIdf: Broadcast[Map[String, Map[String, Float]]]) = {
		// TODO: refactor using rebuildTransientFields?
		// create Lucene IndexReader, IndexSearcher and analyzers
		val indexReader = DirectoryReader.open(directory)
		val similarity = new BM25WithGlobalIDFSimilarity(globalIdf.value)
		val indexSearcher = new IndexSearcher(indexReader)
		indexSearcher.setSimilarity(similarity)
		val indexTimeAnalyzer = config.getIndexTimeAnalyzer
		val queryTimeAnalyzer = config.getQueryTimeAnalyzer
		val queryConstructor = config.getQueryConstructor
		
		// instantiate LuceneIndex
		new GlobalIDFLuceneIndex(directory, config, globalIdf, indexReader, indexSearcher, indexTimeAnalyzer, queryTimeAnalyzer, queryConstructor)
	}
}
