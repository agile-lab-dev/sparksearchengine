package it.agilelab.bigdata.spark.search.impl


import scala.collection.mutable

import it.agilelab.bigdata.spark.search._
import it.agilelab.bigdata.spark.search.dsl.DslQuery
import org.apache.lucene.document.DateTools.Resolution
import org.apache.lucene.document.DateTools._
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document.{DoublePoint, FloatPoint, IntPoint, LongPoint, StoredField, Document => LuceneDocument, StringField => LuceneStringField, TextField => LuceneTextField}
import org.apache.lucene.index.{ConcurrentMergeScheduler, IndexWriter, IndexWriterConfig, TieredMergePolicy}
import org.apache.spark.util.SizeEstimator
import scala.collection.mutable.ArrayBuilder
import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
	* Created by m1lt0n on 29/02/16.
	*/
class LuceneIndexedPartition[T] private[search](
		private[impl] val partitionElements: Array[T],
		private[impl] val partitionIndex: LuceneIndex,
		private[impl] val numPartitions: Int)
	extends IndexedPartition[T] {
	private[search] def iterator: Iterator[T] = partitionElements.iterator

	override def getElement(id: Long): Option[T] = {
		val elementNum = (id / numPartitions).toInt
		if (elementNum < partitionElements.length) { // id is valid
			Some(partitionElements(elementNum))
		} else { // id is invalid
			None
		}
	}

	override def search(query: Query, maxHits: Int): Iterator[(T, Double)] = {
		elementResults(partitionIndex.searchAny(query, maxHits))
	}

	override def search[V](query: Query, maxHits: Int, transfomer: T => V): Iterator[(V, Double)] = {
		elementResultsWithTransformer(partitionIndex.searchAny(query, maxHits), transfomer)
	}

	override def lightSearch(query: Query, maxHits: Int): Iterator[(Long, Double)] = {
		partitionIndex.searchAny(query, maxHits)
	}

	override def batchSearch(queries: Iterator[(Long, DslQuery)], maxHits: Int): Iterator[(Long, Iterator[(T, Double)])] = {
		elementBatchResults(partitionIndex.batchSearch(queries.toIterable, maxHits))
	}

	override def batchSearch[V](queries: Iterator[(Long, DslQuery)], maxHits: Int, transformer: T => V): Iterator[(Long, Iterator[(V, Double)])] = {
		elementBatchResultsWithTransformer(partitionIndex.batchSearch(queries.toIterable, maxHits), transformer)
	}

	override def lightBatchSearch(queries: Iterator[(Long, DslQuery)], maxHits: Int): Iterator[(Long, Iterator[(Long, Double)])] = {
		partitionIndex.batchSearch(queries.toIterable, maxHits)
	}

	override def batchSearchRaw(queries: Iterator[(Long, RawQuery)], maxHits: Int): Iterator[(Long, Iterator[(T, Double)])] = {
		elementBatchResults(partitionIndex.batchSearchRaw(queries, maxHits))
	}

	override def batchSearchRaw[V](queries: Iterator[(Long, RawQuery)], maxHits: Int, transformer: T => V): Iterator[(Long, Iterator[(V, Double)])] = {
		elementBatchResultsWithTransformer(partitionIndex.batchSearchRaw(queries, maxHits), transformer)
	}

	override def lightBatchSearchRaw(queries: Iterator[(Long, RawQuery)], maxHits: Int): Iterator[(Long, Iterator[(Long, Double)])] = {
		partitionIndex.batchSearchRaw(queries, maxHits)
	}

	/**
		* Substitutes element numbers in the results with the respective elements.
		*
		* @param results
		* @return
		*/
	private def elementResults(results: Iterator[(Long, Double)]): Iterator[(T, Double)] = {
		results map { case (elementId, score) => {
				// elementId was generated with zipWithUniqueId; so the only thing we
			  // need to do to recover its index is divide it by the number of partitions
				// and cast it to Int
				val elementNum = (elementId/numPartitions).toInt
			  (partitionElements(elementNum), score)
		  }
		}
	}

	private def elementBatchResults(batchResults: Iterator[(Long, Iterator[(Long, Double)])]): Iterator[(Long, Iterator[(T, Double)])] = {
		batchResults map { case (queryId, results) => (queryId, elementResults(results)) }
	}

	private def elementResultsWithTransformer[V](results: Iterator[(Long, Double)], transformer: T => V): Iterator[(V, Double)] = {
		results map { case (elementId, score) => {
				// elementId was generated with zipWithUniqueId; so the only thing we
				// need to do to recover its index is divide it by the number of partitions
				// and cast it to Int
				val elementNum = (elementId/numPartitions).toInt
				(transformer(partitionElements(elementNum)), score)
			}
		}
	}

	private def elementBatchResultsWithTransformer[V](batchResults: Iterator[(Long, Iterator[(Long, Double)])], transformer: T => V): Iterator[(Long, Iterator[(V, Double)])] = {
		batchResults map { case (queryId, results) => (queryId, elementResultsWithTransformer(results, transformer)) }
	}

	def getIndexedPartitionInfo: String = {
		val elementsSize = SizeEstimator.estimate(partitionElements)
		val indexInfo = partitionIndex.getIndexInfo
		f"Data: ${partitionElements.length} elements, $elementsSize bytes. " +
		f"Index: ${indexInfo.numSegments} segments, ${indexInfo.numDocuments} documents, ${indexInfo.sizeBytes} bytes."
	}
}

object LuceneIndexedPartition {
	import PartitionsIndexLuceneRDD._

	private[search] def createLuceneIndexedPartition[T](
      zippedElements: Iterator[(T, Long)],
      config: LuceneConfig,
      numParts: Int)
		 (implicit ct: ClassTag[T],
	    toIndexable: T => Indexable)
		 : LuceneIndexedPartition[T] = {
		// create Lucene Directory and IndexWriter according to config
		val directory = new BigChunksRAMDirectory()
		val indexWriterConfig = buildIndexWriterConfig(config)
		val indexWriter = new IndexWriter(directory, indexWriterConfig)

		// index elements, adding them to storage array at the same time
		val partitionElements = new ArrayBuilder.ofRef[T with AnyRef]().asInstanceOf[ArrayBuilder[T]]
		for (zippedElement <- zippedElements) {
			val (element, id) = zippedElement
			val document = indexable2LuceneDocument(element)
			document.add(new StoredField(ElementId, id))
			indexWriter.addDocument(document)
			partitionElements += element
		}

		// compact index to one segment, commit & close
		indexWriter.getConfig.setUseCompoundFile(true)
		if (config.getCompactIndex) indexWriter.forceMerge(1, true)
		indexWriter.close()

		// instantiate LuceneIndexedPartition
		new LuceneIndexedPartition(partitionElements.result(), LuceneIndex(directory, config), numParts)
	}
	
	private[search] def createLuceneIndexedPartitionStoreable[T](zippedElements: Iterator[(Storeable[T], Long)],
		                                                  config: LuceneConfig,
		                                                  numParts: Int)
	                                                   (implicit ct: ClassTag[Storeable[T]],
	                                                    ct2: ClassTag[T])
																										 : LuceneIndexedPartition[T] = {
		// create Lucene Directory and IndexWriter according to config
		val directory = new BigChunksRAMDirectory()
		val indexWriterConfig = buildIndexWriterConfig(config)
		val indexWriter = new IndexWriter(directory, indexWriterConfig)
		
		// index elements, adding them to storage array at the same time
		val partitionElements = new ArrayBuilder.ofRef[T with AnyRef]().asInstanceOf[ArrayBuilder[T]]
		for (zippedElement <- zippedElements) {
			val (element, id) = zippedElement
			val document = indexable2LuceneDocument(element)
			document.add(new StoredField(ElementId, id))
			indexWriter.addDocument(document)
			partitionElements += element.getData
		}
		
		// compact index to one segment, commit & close
		indexWriter.getConfig.setUseCompoundFile(true)
		if (config.getCompactIndex) indexWriter.forceMerge(1, true)
		indexWriter.close()
		
		// instantiate LuceneIndexedPartition
		new LuceneIndexedPartition(partitionElements.result(), LuceneIndex(directory, config), numParts)
	}
	
	implicit def indexable2LuceneDocument(indexable: Indexable): LuceneDocument = {
		val luceneDocument = new LuceneDocument()

		indexable.getFields foreach {
			field =>
				if (field.name.charAt(0) == SpecialFieldMarker) {
					throw new IllegalArgumentException("Field names cannot start with '" + SpecialFieldMarker + "'")
				}
				field match {
					case s: StringField => luceneDocument.add(new LuceneTextField(s.name, s.value, Store.NO))
					case nps: NoPositionsStringField => luceneDocument.add(new NoPositionsTextField(nps.name, nps.value, Store.NO))
					case i: IntField    => luceneDocument.add(new IntPoint(i.name, i.value))
					case l: LongField   => luceneDocument.add(new LongPoint(l.name, l.value))
					case f: FloatField  => luceneDocument.add(new FloatPoint(f.name, f.value))
					case d: DoubleField => luceneDocument.add(new DoublePoint(d.name, d.value))
					case dt: DateField  => luceneDocument.add(new LuceneStringField(dt.name, dateToString(dt.value, Resolution.SECOND), Store.NO))
					case sq: SeqField   => luceneDocument.add(new LuceneTextField(sq.name, sq.value.toString, Store.NO))
				}
		}

		luceneDocument
	}
	
	/** Build a Lucene IndexWriterConfig using our LuceneConfig.
		*/
	def buildIndexWriterConfig(config: LuceneConfig): IndexWriterConfig = {
		val analyzer = config.getIndexTimeAnalyzer
		val indexWriterConfig = new IndexWriterConfig(analyzer)
		
		val mergePolicy = new TieredMergePolicy()
		val mergeScheduler = new ConcurrentMergeScheduler()
		mergeScheduler.setDefaultMaxMergesAndThreads(false)
		mergeScheduler.disableAutoIOThrottle()
		indexWriterConfig.setMergePolicy(mergePolicy)
		indexWriterConfig.setMergeScheduler(mergeScheduler)
		
		indexWriterConfig.setRAMBufferSizeMB(64d)
		
		indexWriterConfig.setUseCompoundFile(false)
		
		indexWriterConfig.setCommitOnClose(true)
		
		val customCodec = new CustomCodec()
		indexWriterConfig.setCodec(customCodec)
		
		val similarity = config.getSimilarity
		indexWriterConfig.setSimilarity(similarity)
		
		indexWriterConfig
	}
}
