package it.agilelab.bigdata.spark.search.impl

import java.io.{IOException, ObjectInputStream}

import scala.collection.mutable
import scala.collection.JavaConverters._

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import it.agilelab.bigdata.spark.search._
import it.agilelab.bigdata.spark.search.dsl.{DslQuery, NegatedQuery}
import it.agilelab.bigdata.spark.search.impl.queries.QueryConstructor
import it.agilelab.bigdata.spark.search.utils.IDFUtils
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.{IndexSearcher, Query => LuceneQuery}
import org.apache.lucene.util.BytesRef

/**
  * Lucene-based implementation of {@link it.agilelab.bigdata.spark.search.Index}.
  */
@SerialVersionUID(1l)
class LuceneIndex private[impl](
		private[impl] var directory: BigChunksRAMDirectory,
		private[impl] var config: LuceneConfig,
		@transient private var indexReader: DirectoryReader,
		@transient private var indexSearcher: IndexSearcher,
		@transient private var indexTimeAnalyzer: Analyzer,
		@transient private var queryTimeAnalyzer: Analyzer,
		@transient private var queryConstructor: QueryConstructor)
	extends Index with KryoSerializable {
	import IDFUtils.calculateTermIDFs
	import PartitionsIndexLuceneRDD._
 
	override def searchAny(query: Query, maxHits: Int): Iterator[(Long, Double)] = {
		query match {
			case dsl: DslQuery => search(dsl, maxHits)
			case raw: RawQuery => search(raw, maxHits)
		}
	}

	override def search(query: DslQuery, maxHits: Int): Iterator[(Long, Double)] = {
		val luceneQuery = query match {
			case not: NegatedQuery => not.rewriteToAllAndThis(queryTimeAnalyzer)
			case _ => query.getLuceneQuery(queryTimeAnalyzer)
		}
		search(luceneQuery, maxHits)
	}

	override def search(query: RawQuery, maxHits: Int): Iterator[(Long, Double)] = {
		search(queryConstructor.constructQuery(query), maxHits)
	}

	override def batchSearch(queries: Iterable[(Long, DslQuery)], maxHits: Int): Iterator[(Long, Iterator[(Long, Double)])] = {
		queries map { case (id, query) => (id, search(query, maxHits)) } iterator
	}

	override def batchSearchRaw(queries: Iterator[(Long, RawQuery)], maxHits: Int): Iterator[(Long, Iterator[(Long, Double)])] = {
		queries map { case (id, query) => (id, search(query, maxHits)) }
	}

	private def search(luceneQuery: LuceneQuery, maxHits: Int): Iterator[(Long, Double)] = {
		if (luceneQuery != null) {
			val results = indexSearcher.search(luceneQuery, maxHits).scoreDocs map {
				scoreDoc =>
					val id = indexReader.document(scoreDoc.doc).getField(ElementId).numericValue().longValue()
					val score = scoreDoc.score.toDouble
					(id, score)
			}
			results.iterator
		} else {
			Iterator[(Long,Double)]()
		}
	}

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
	}

	/**
		* Deserialize this object using kryo.
		*/
	override def read(kryo: Kryo, input: Input): Unit = {
		// read non-transient fields
		directory = kryo.readObject(input, classOf[BigChunksRAMDirectory])
		config = kryo.readObject(input, classOf[LuceneConfig])
		// rebuild transient fields
		this.rebuildTransientFields()
	}

	private def rebuildTransientFields(): Unit = {
		// create Lucene IndexReader, IndexSearcher and analyzers
		indexReader = DirectoryReader.open(directory)
		indexSearcher = new IndexSearcher(indexReader)
		indexTimeAnalyzer = config.getIndexTimeAnalyzer
		queryTimeAnalyzer = config.getQueryTimeAnalyzer
		queryConstructor = config.getQueryConstructor
	}
	
	/**
		* Gets information about this index: number of segments, number of documents, bytes used.
		*/
	def getIndexInfo: IndexInfo = IndexInfo(indexReader.numDocs(), directory.ramBytesUsed(), indexReader.getContext.children().size())
	
	/**
		* Returns the document counts for this index.
		* The key for the map is the field name; the value is the number of documents.
		*/
	override def getDocumentCounts: Map[String, Long] = {
		// obtain leaf readers
		val leafReaders = indexReader.getContext.leaves.asScala.map(leaf => leaf.reader())
		
		// obtain field set from leaf readers
		val fields = leafReaders.flatMap(leafReader => leafReader.fields().asScala).toSet
		
		// obtain document counts
		val docCounts = fields.map(field => field -> indexSearcher.collectionStatistics(field).docCount())
		
		docCounts.toMap
	}
	
	/**
		* Returns the terms counts for this index.
		* The key for the first map is the field name; the one for the second map is the term; the value is the number of
		* occurrences of the term in that field across all documents.
		*/
	override def getTermCounts: Map[String, Map[String, Long]] = {
		// obtain leaf readers
		val leafReaders = indexReader.getContext.leaves.asScala.map(leaf => leaf.reader())
		
		// obtain field set from leaf readers
		val fields = leafReaders.flatMap(leafReader => leafReader.fields().asScala).toSet
		
		// create map
		val fieldsTermFreqMap = mutable.Map[String, mutable.Map[String, Long]]()
		fields.foreach(field => fieldsTermFreqMap += field -> mutable.Map[String, Long]())
		
		// for each field and each leaf reader
		for (field <- fields; leafReader <- leafReaders) yield {
			// get the map for the field
			val fieldTermFreqMap = fieldsTermFreqMap(field)
			
			// get the terms
			val terms = leafReader.terms(field).iterator()
			
			// horrid while loop, due to lucene interface
			var term: BytesRef = terms.next()
			while (term != null) {
				val termString = term.utf8ToString() // FIXME we assume the term is always string!
				
				// add term and its info to field map, summing with preexisting values
				val existingValue = fieldTermFreqMap.getOrElse(termString, 0l)
				fieldTermFreqMap += termString -> (existingValue + terms.totalTermFreq())
				
				term = terms.next()
			}
		}
		
		// convert to immutable maps
		val immutableMap = fieldsTermFreqMap.map(x => x._1 -> x._2.toMap).toMap
		
		immutableMap
	}
	
	/**
		* Returns the term IDFs for this index.
		* The key for the first map is the field name; the one for the second map is the term; the value is the IDF of
		* of the term in that field.
		*/
	override def getTermIDFs: Map[String, Map[String, Float]] = {
		// obtain document and term counts
		val docCounts = getDocumentCounts
		val termCounts = getTermCounts
		
		calculateTermIDFs(docCounts, termCounts)
	}
}

object LuceneIndex {
	private[search] def apply[T](
			directory: BigChunksRAMDirectory,
			config: LuceneConfig) = {
		// TODO: refactor using rebuildTransientFields?
		// create Lucene IndexReader, IndexSearcher and analyzers
		val indexReader = DirectoryReader.open(directory)
		val indexSearcher = new IndexSearcher(indexReader)
		val indexTimeAnalyzer = config.getIndexTimeAnalyzer
		val queryTimeAnalyzer = config.getQueryTimeAnalyzer
		val queryConstructor = config.getQueryConstructor

		// instantiate LuceneIndex
		new LuceneIndex(directory, config, indexReader, indexSearcher, indexTimeAnalyzer, queryTimeAnalyzer, queryConstructor)
	}
}