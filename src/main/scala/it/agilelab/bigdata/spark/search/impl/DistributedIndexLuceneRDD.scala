package it.agilelab.bigdata.spark.search.impl

import scala.reflect.ClassTag

import it.agilelab.bigdata.spark.search._
import it.agilelab.bigdata.spark.search.dsl.DslQuery
import it.agilelab.bigdata.spark.search.utils._
import org.apache.lucene.document.StoredField
import org.apache.lucene.index.IndexWriter
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{OneToOneDependency, Partition, TaskContext}

/**
	* SearchableRDD implementation using Lucene. Indices are kept in a separate, independent RDD from the data.
	*/
class DistributedIndexLuceneRDD[T] private[search] (val indexRDD: RDD[LuceneIndex], val elementsRDD: RDD[(Long, T)])
   (implicit ct: ClassTag[T])
	extends SearchableRDD[T] (elementsRDD.context, List(new OneToOneDependency(elementsRDD))) {
	import SearchableRDD._
	
	/*
		===============================================================================================
		===============================================================================================
			SearchableRDD methods implementations
		===============================================================================================
		===============================================================================================
	*/
	
	/** Returns an `Array` of elements matching the `query` with their scores, returning at most
		* `maxHits` results.
		*
		* Setting `maxHitsPerIndex` limits the size of intermediate results. It must be a positive value
		* between `1` and `maxHits` (both inclusive). The (internally handled) default is `maxHits`.
		*
		* The resulting `Array` is produced by efficiently aggregating partial results in a tree-level
		* pattern.
		*
		* @note $intermediateResults
		* @see [[org.apache.spark.rdd.RDD#treeAggregate]]
		* @group Search
		*/
	override def aggregatingSearch(query: Query, maxHits: Int, maxHitsPerIndex: Int): Array[(T, Double)] = {
		val maxHitsPerIndexActual = getMHPIActual(maxHitsPerIndex, maxHits)
		val results = indexRDD.map(_.searchAny(query, maxHitsPerIndexActual))
		val flattenedResults = results.flatMap(identity)
		val resultsJoinedWithElements = flattenedResults.join(elementsRDD).map(_._2.swap)
		val groupedResults = resultsJoinedWithElements.mapPartitions(it => Iterator(it))
		aggregateResults(groupedResults, maxHits)
	}
	
	/** Like [[aggregatingSearch(query:it\.agilelab\.bigdata\.spark\.search\.Query,maxHits:Int,maxHitsPerIndex:Int)* aggregatingSearch(Query,Int,Int)]],
		* but specifying a `transformer` function to be applied to the result elements.
		*
		* @group Search
		*/
	override def aggregatingSearch[V](query: Query, maxHits: Int, transformer: (T) => V, maxHitsPerIndex: Int): Array[(V, Double)] = {
		val maxHitsPerIndexActual = getMHPIActual(maxHitsPerIndex, maxHits)
		val results = indexRDD.map(_.searchAny(query, maxHitsPerIndexActual))
		val flattenedResults = results.flatMap(identity)
		val resultsJoinedWithElements = flattenedResults.join(elementsRDD).map { case (elementId, (score, element)) => (transformer(element), score) }
		val groupedResults = resultsJoinedWithElements.mapPartitions(it => Iterator(it))
		aggregateResults(groupedResults, maxHits)
	}
	
	/** Like [[aggregatingSearch(query:it\.agilelab\.bigdata\.spark\.search\.Query,maxHits:Int,maxHitsPerIndex:Int)* aggregatingSearch(Query,Int,Int)]],
		* but returns the ids of the resulting elements.
		*
		* @note $ephemeralIDs
		* @group Search
		*/
	override def lightAggregatingSearch(query: Query, maxHits: Int, maxHitsPerIndex: Int): Array[(Long, Double)] = {
		val maxHitsPerIndexActual = getMHPIActual(maxHitsPerIndex, maxHits)
		val results = indexRDD.map(_.searchAny(query, maxHitsPerIndexActual))
		aggregateResults(results, maxHits)
	}
	
	/** Returns an `Array` built by aggregating the `Iterator`s in `result` by repetitively choosing
		* the top `maxHits` results in descending score order.
		*
		* The aggregation is done in a tree-level pattern.
		*
		* @see [[org.apache.spark.rdd.RDD#treeAggregate]]
		*/
	private def aggregateResults[E](results: RDD[Iterator[(E, Double)]], maxHits: Int): Array[(E, Double)] = {
		results.treeAggregate(Array[(E, Double)]())(
			{
				case (res1, res2) => merge(res1, res2.toArray, maxHits)
			},
			{
				case (res1, res2) => merge(res1, res2, maxHits)
			}
		)
	}
	
	/** Returns an `RDD` of elements matching the `query`, returning at most `maxHits` results.
		*
		* Setting `maxHitsPerIndex` limits the size of intermediate results. It must be a positive value
		* between `1` and `maxHits` (both inclusive). The (internally handled) default is `maxHits`.
		*
		* @note $intermediateResults
		* @group Search
		*/
	override def search(query: Query, maxHits: Int, maxHitsPerIndex: Int): RDD[(T, Double)] = {
		val mhpiActual = getMHPIActual(maxHitsPerIndex, maxHits)
		val results = indexRDD.flatMap(index => index.searchAny(query, mhpiActual))
		val sortedResults = sortResults(results, maxHits)
		sortedResults.join(elementsRDD)
			.map { case (_, (score, element)) => (element, score)}
	}
	
	/** Like [[search(query:it\.agilelab\.bigdata\.spark\.search\.Query,maxHits:Int,maxHitsPerIndex:Int)* search(Query,Int,Int)]],
		* but specifying a `transformer` function to be applied to the result elements.
		*
		* @group Search
		*/
	override def search[V](query: DslQuery, maxHits: Int, transformer: (T) => V, maxHitsPerIndex: Int): RDD[(V, Double)] = {
		val mhpiActual = getMHPIActual(maxHitsPerIndex, maxHits)
		val results = indexRDD.flatMap(index => index.searchAny(query, mhpiActual))
		val sortedResults = sortResults(results, maxHits)
		sortedResults.join(elementsRDD)
			.map { case (_, (score, element)) => (transformer(element), score)}
	}
	
	/** Like [[search(query:it\.agilelab\.bigdata\.spark\.search\.Query,maxHits:Int,maxHitsPerIndex:Int)* search(Query,Int,Int)]],
		* but returns the ids of the resulting elements.
		*
		* @note $ephemeralIDs
		* @group Search
		*/
	override def lightSearch(query: Query, maxHits: Int, maxHitsPerIndex: Int): RDD[(Long, Double)] = {
		val results = indexRDD.flatMap(index => index.searchAny(query, maxHitsPerIndex))
		sortResults(results, maxHits)
	}
	
	/** Returns an `RDD` of elements matching the `queries`, returning at most `maxHits` results for
		* each query.
		*
		* The queries must be provided with a unique id for each query, as an `Iterator` of `(id, query)`
		* pairs. The results of each query are returned associated with this unique id as `(id, results)`
		* pairs in the returned RDD; each `results` is an `Array` of `(element, score)` pairs.
		*
		* Setting `maxHitsPerIndex` limits the size of intermediate results. It must be a positive value
		* between `1` and `maxHits` (both inclusive). The (internally handled) default is `maxHits`.
		*
		* @note $intermediateResults
		* @group Search
		*/
	override def batchSearch(queries: Iterable[(Long, DslQuery)], maxHits: Int, maxHitsPerIndex: Int): RDD[(Long, Array[(T, Double)])] = {
		val mhpiActual = getMHPIActual(maxHitsPerIndex, maxHits)
		val results = indexRDD.flatMap(_.batchSearch(queries, mhpiActual))
		val flattenedResults = results flatMap { case (query, queryResults) => queryResults map { case (elementId, score) => (elementId, (score, query)) } }
		val resultsJoinedWithElements = flattenedResults.join(elementsRDD)
		val groupedResults = resultsJoinedWithElements map {
				case (elementId, ((score, query), element)) => (query, (element, score))
			} groupByKey()
		val sortedLimitedArrayResults = groupedResults map {
			case (query, queryResults) =>
				(query, queryResults.toArray.sortBy(_._2).reverse.take(maxHits)) // FIXME optimize sorting/results cutoff
		}
		
		sortedLimitedArrayResults
	}
	
	/** Like [[batchSearch(queries:Iterator[(Long,it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery)],maxHits:Int,maxHitsPerIndex:Int)* batchSearch(Iterator[(Long,DslQuery)],Int,Int)]],
		* but specifying a `transformer` function to be applied to the result elements.
		*
		* @group Search
		*/
	override def batchSearch[V](queries: Iterator[(Long, DslQuery)], maxHits: Int, transformer: (T) => V, maxHitsPerIndex: Int): RDD[(Long, Array[(V, Double)])] = {
		val mhpiActual = getMHPIActual(maxHitsPerIndex, maxHits)
		val results = indexRDD.flatMap(_.batchSearch(queries.toIterable, mhpiActual))
		val flattenedResults = results flatMap { case (query, queryResults) => queryResults map { case (elementId, score) => (elementId, (score, query)) } }
		val resultsJoinedWithElements = flattenedResults.join(elementsRDD)
		val groupedResults = resultsJoinedWithElements map {
			case (elementId, ((score, query), element)) => (query, (transformer(element), score))
		} groupByKey()
		val sortedLimitedArrayResults = groupedResults map {
			case (query, queryResults) =>
				(query, queryResults.toArray.sortBy(_._2).reverse.take(maxHits)) // FIXME optimize sorting/results cutoff
		}
		
		sortedLimitedArrayResults
	}
	
	/** Like [[batchSearch(queries:Iterator[(Long,it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery)],maxHits:Int,maxHitsPerIndex:Int)* batchSearch(Iterator[(Long,DslQuery)],Int,Int)]],
		* but returns the ids of the resulting elements.
		*
		* @note $ephemeralIDs
		* @group Search
		*/
	override def lightBatchSearch(queries: Iterator[(Long, DslQuery)], maxHits: Int, maxHitsPerIndex: Int): RDD[(Long, Array[(Long, Double)])] = {
		val mhpiActual = getMHPIActual(maxHitsPerIndex, maxHits)
		val results = indexRDD.flatMap(_.batchSearch(queries.toIterable, mhpiActual))
		aggregateResultsByKey(results, maxHits)
	}
	
	/** Like [[batchSearch(queries:Iterator[(Long,it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery)],maxHits:Int,maxHitsPerIndex:Int)* batchSearch(Iterator[(Long,DslQuery)],Int,Int)]],
		* but uses [[RawQuery]] type queries instead of [[dsl.DslQuery]].
		*
		* @group Search
		*/
	override def batchSearchRaw(queries: Iterator[(Long, RawQuery)], maxHits: Int, maxHitsPerIndex: Int): RDD[(Long, Array[(T, Double)])] = {
		val mhpiActual = getMHPIActual(maxHitsPerIndex, maxHits)
		val results = indexRDD.flatMap(_.batchSearchRaw(queries, mhpiActual))
		val flattenedResults = results flatMap { case (query, queryResults) => queryResults map { case (elementId, score) => (elementId, (score, query)) } }
		val resultsJoinedWithElements = flattenedResults.join(elementsRDD)
		val groupedResults = resultsJoinedWithElements map {
			case (elementId, ((score, query), element)) => (query, (element, score))
		} groupByKey()
		val sortedLimitedArrayResults = groupedResults map {
			case (query, queryResults) =>
				(query, queryResults.toArray.sortBy(_._2).reverse.take(maxHits)) // FIXME optimize sorting/results cutoff
		}
		
		sortedLimitedArrayResults
	}
	
	/** Like [[batchSearch[V](queries:Iterator[(Long,it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery)],maxHits:Int,transformer:T=>V,maxHitsPerIndex:Int)* batchSearch(Iterator[(Long,DslQuery)],Int,T=>V,Int)]],
		* but uses [[RawQuery]] type queries instead of [[dsl.DslQuery]].
		*
		* @group Search
		*/
	override def batchSearchRaw[V](queries: Iterator[(Long, RawQuery)], maxHits: Int, transformer: (T) => V, maxHitsPerIndex: Int): RDD[(Long, Array[(V, Double)])] = {
		val mhpiActual = getMHPIActual(maxHitsPerIndex, maxHits)
		val results = indexRDD.flatMap(_.batchSearchRaw(queries, mhpiActual))
		val flattenedResults = results flatMap { case (query, queryResults) => queryResults map { case (elementId, score) => (elementId, (score, query)) } }
		val resultsJoinedWithElements = flattenedResults.join(elementsRDD)
		val groupedResults = resultsJoinedWithElements map {
			case (elementId, ((score, query), element)) => (query, (transformer(element), score))
		} groupByKey()
		val sortedLimitedArrayResults = groupedResults map {
			case (query, queryResults) =>
				(query, queryResults.toArray.sortBy(_._2).reverse.take(maxHits)) // FIXME optimize sorting/results cutoff
		}
		
		sortedLimitedArrayResults
	}
	
	/** Like [[lightBatchSearch(queries:Iterator[(Long,it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery)],maxHits:Int,maxHitsPerIndex:Int)* lightBatchSearch(Iterator[(Long,DslQuery)],Int,Int)]],
		* but uses [[RawQuery]] type queries instead of [[dsl.DslQuery]].
		*
		* @group Search
		*/
	override def lightBatchSearchRaw(queries: Iterator[(Long, RawQuery)], maxHits: Int, maxHitsPerIndex: Int): RDD[(Long, Array[(Long, Double)])] = {
		val mhpiActual = getMHPIActual(maxHitsPerIndex, maxHits)
		val results = indexRDD.flatMap(_.batchSearchRaw(queries, mhpiActual))
		aggregateResultsByKey(results, maxHits)
	}
	
	/** Returns an `RDD` that contains the result of the query join between `this` and `other`.
		*
		* A query join is a join in which the predicate is a query. From each element of `other`, a query
		* is obtained by applying `queryGenerator`. Then each query is ran against `this`, and the
		* `maxHits` results produced are joined with the element originating the query.
		*
		* Queries are executed in batches, with a batch for each partition of `other`.
		*
		* @group QueryJoin
		*/
	override def queryJoin[U: ClassManifest](other: RDD[U], queryGenerator: (U) => DslQuery, maxHits: Int): RDD[(U, Array[(T, Double)])] = {
		// assign id to elements
		val elementsWithId = other.zipWithUniqueId().map(_.swap)
		
		// create queries
		val queries = generateQueries(elementsWithId, queryGenerator)
		
		// do the search
		val resultsIds = queries.cartesian(indexRDD) map {
			case (queryBatch, index) => index.batchSearch(queryBatch.toIterable, maxHits)
		}
		resultsIds.setName(f"Search results (cartesian) [${resultsIds.id}]")
		
		// join results with data
		val results = resultsIds.flatMap(identity(_))
		val flattenedResults = results flatMap { case (query, queryResults) => queryResults map { case (elementId, score) => (elementId, (score, query)) } }
		val resultsJoinedWithElements = flattenedResults.join(elementsRDD)
		val groupedResults = resultsJoinedWithElements map {
			case (elementId, ((score, query), element)) => (query, (element, score))
		} groupByKey()
		
		// process results
		val sortedLimitedArrayResults = groupedResults map {
			case (query, queryResults) =>
				(query, queryResults.toArray.sortBy(_._2).reverse.take(maxHits)) // FIXME optimize sorting/results cutoff
		}
		
		// combine results with elements originating the queries
		val combined = joinQueriesResults(elementsWithId, sortedLimitedArrayResults)
		
		combined
	}
	
	/** Like [[queryJoin[U](other:org\.apache\.spark\.rdd\.RDD[U],queryGenerator:U=>it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery,maxHits:Int)* queryJoin[U](RDD[U],U=>DslQuery,Int)]],
		* but specifying a `transformer` function to be applied to the result elements.
		*
		* @group QueryJoin
		*/
	override def queryJoin[U: ClassManifest, V: ClassManifest](other: RDD[U], queryGenerator: (U) => DslQuery, maxHits: Int, transformer: (T) => V): RDD[(U, Array[(V, Double)])] = {
		// assign id to elements
		val elementsWithId = other.zipWithUniqueId().map(_.swap)
		
		// create queries
		val queries = generateQueries(elementsWithId, queryGenerator)
		
		// do the search
		val resultsIds = queries.cartesian(indexRDD) map {
			case (queryBatch, index) => index.batchSearch(queryBatch.toIterable, maxHits)
		}
		resultsIds.setName(f"Search results (cartesian) [${resultsIds.id}]")
		
		// join results with data
		val results = resultsIds.flatMap(identity(_))
		val flattenedResults = results flatMap { case (query, queryResults) => queryResults map { case (elementId, score) => (elementId, (score, query)) } }
		val resultsJoinedWithElements = flattenedResults.join(elementsRDD)
		val groupedResults = resultsJoinedWithElements map {
			case (elementId, ((score, query), element)) => (query, (transformer(element), score))
		} groupByKey()
		
		// process results
		val sortedLimitedArrayResults = groupedResults map {
			case (query, queryResults) =>
				(query, queryResults.toArray.sortBy(_._2).reverse.take(maxHits)) // FIXME optimize sorting/results cutoff
		}
		
		// combine results with elements originating the queries
		val combined = joinQueriesResults(elementsWithId, sortedLimitedArrayResults)
		
		combined
	}
	
	/** Like [[queryJoin[U](other:org\.apache\.spark\.rdd\.RDD[U],queryGenerator:U=>it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery,maxHits:Int)* queryJoin[U](RDD[U],U=>DslQuery,Int)]],
		* but returns the ids of the resulting elements.
		*
		* @note $ephemeralIDs
		* @group QueryJoin
		*/
	override def lightQueryJoin[U : ClassManifest](other: RDD[U], queryGenerator: (U) => DslQuery, maxHits: Int): RDD[(U, Array[(Long, Double)])] = {
		// assign id to elements
		val elementsWithId = other.zipWithUniqueId().map(_.swap)
		
		// create query batches
		val queries = generateQueries(elementsWithId, queryGenerator)
		
		// do the search, process results
		val results = queries.cartesian(indexRDD) map {
			case (queryBatch, index) => index.batchSearch(queryBatch.toIterable, maxHits)
		}
		results.setName(f"Search results (cartesian) [${results.id}]")
		
		// process results
		val processedResults = processResults(results, maxHits)
		
		// combine results with elements originating the queries
		val combined = joinQueriesResults(elementsWithId, processedResults)
		
		combined
	}
	
	/** Generates queries by applying `queryGenerator` to the elements of `elementsWithID`. Queries
		* are grouped in one batch for each partition.
		*/
	private def generateQueries[U](elementsWithId: RDD[(Long, U)],
	                               queryGenerator: U => DslQuery)
	: RDD[Iterator[(Long, DslQuery)]] = {
		// apply query generation function
		val queries = elementsWithId map {
			case (elementId, element) => (elementId, queryGenerator(element))
		}
		// batch queries
		val queriesBatch = queries.glom() map { array => array.iterator}
		// set name
		val parentName = elementsWithId.dependencies.head.rdd.name
		val queriesRDDName = "Queries" + (if (parentName != null) " (from " + parentName + ")" else "") + f" [${queriesBatch.id}]"
		queriesBatch.setName(queriesRDDName)
		
		queriesBatch
	}
	
	/** Returns the `RDD` obtained by processing `results` by flattening it, then aggregating the results
		* for each id into a single `Array` of the top `maxHits` results in descending score order.
		*/
	private def processResults[V: ClassTag](results: RDD[Iterator[(Long, Iterator[(V, Double)])]],
	                                        maxHits: Int)
	: RDD[(Long, Array[(V, Double)])] = {
		// flatten results
		val flatResults = results.flatMap(identity(_))
		
		// aggregate flattened results
		val aggregatedResults = flatResults.aggregateByKey(Array[(V, Double)]())(
			{
				case (res1, res2) => merge(res1, res2.toArray, maxHits)
			},
			{
				case (res1, res2) => merge(res1, res2, maxHits)
			})
		// set name
		aggregatedResults.setName(f"Search results (aggregated) [${aggregatedResults.id}]")
		
		aggregatedResults
	}
	
	/** Returns the RDD produced by joining  by id the results in `flatResults` with the elements
		* originating the queries that produced them in `elementsWithId`.
		*/
	private def joinQueriesResults[U: ClassTag, V: ClassTag](elementsWithId: RDD[(Long, U)],
	                                                         flatResults: RDD[(Long, Array[(V, Double)])])
	: RDD[(U, Array[(V, Double)])] = {
		// join elements originating queries with their results via query id
		val joined = elementsWithId.join(flatResults)
		// throw id away
		val joinedNoId = joined map {
			case (_, elementWithResults) => elementWithResults
		}
		// set name
		joinedNoId.setName(f"Query join result [${joinedNoId.id}]")
		
		joinedNoId
	}
	
	/** Returns the element of this `SearchableRDD` with corresponds to the provided `id`, if it exists. */
	override def getElement(id: Long): Option[T] = {
		// the ids are generated with zipWithUniqueIds, therefore we
		// can easily find out the partition to which id belongs by
		// calculating id modulo number of partitions
		val index = (id % elementsRDD.getNumPartitions).toInt
		// function to process a partition to get the element back
		val process = (iterator: Iterator[(Long, T)]) => {
			iterator.find(el => el._1 == id).map(_._2)
		} : Option[T]
		// run a job against the partition containing id, return the first and only result
		val res = this.context.runJob[(Long, T), Option[T]](elementsRDD, process, Array(index))
		res(0)
	}
	
	/** Like [[getElement(id:Long)* getElement(Long)]], but for an `Array` of ids. */
	override def getElements(ids: Array[Long]): Array[(Long, Option[T])] = {
		// the ids are generated with zipWithUniqueIds, therefore we
		// can easily find out the partition to which id belongs by
		// calculating id modulo number of partitions
		val partitionIndices = ids.toSet.map(_ % elementsRDD.getNumPartitions).toArray
		// get a set of ids for fast matching
		val idSet = ids.toSet
		val results = elementsRDD.mapPartitionsWithIndex {
			case (partitionIndex, elements) => {
				// get the subset of ids that are in this partition
				val partitionIdSet = idSet.filter(_ % elementsRDD.getNumPartitions == partitionIndex)
				// get the elements that actually exist and the ids of those that don't
				val results = elements.filter(id_el => partitionIdSet(id_el._1))
				val nonExistent = partitionIdSet.diff(results.map(_._1).toSet)
				// map results to some, the rest to none
				results.map { case (elId, el) => (elId, Some(el)) } ++ nonExistent.map((_, None))
			}
		}
		results.collect()
	}
	
	override def getDocumentCounts: Map[String, Long] = indexRDD.map(_.getDocumentCounts).treeReduce(combineDocumentCountsMaps)
	
	override def getTermCounts: Map[String, Map[String, Long]] = indexRDD.map(_.getTermCounts).treeReduce(combineTermCountsMaps)
	
	override def getIndicesInfo: IndicesInfo = {
		val indexInfos = indexRDD.map(_.getIndexInfo).collect().toList
		
		val totDocs = indexInfos.map(_.numDocuments.toLong).sum
		val totBytes = indexInfos.map(_.sizeBytes).sum
		
		IndicesInfo(indexInfos.size, totDocs, totBytes, indexInfos)
	}
	
	/*
	  ===============================================================================================
    ===============================================================================================
	  	RDD methods implementations
    ===============================================================================================
	  ===============================================================================================
	 */
	
	@DeveloperApi
	override def compute(split: Partition, context: TaskContext): Iterator[T] = elementsRDD.compute(split, context).map(_._2)
	
	override protected def getPartitions: Array[Partition] = elementsRDD.partitions
	
	override def persist(newLevel: StorageLevel): DistributedIndexLuceneRDD.this.type = {
		elementsRDD.persist(newLevel)
		indexRDD.persist(newLevel)
		
		this
	}
	
	override def unpersist(blocking: Boolean = true): DistributedIndexLuceneRDD.this.type = {
		elementsRDD.unpersist(blocking)
		indexRDD.unpersist(blocking)
		
		this
	}
}

object DistributedIndexLuceneRDD {
	import SearchableRDD._
	import LuceneIndexedPartition.indexable2LuceneDocument
	import LuceneIndexedPartition.buildIndexWriterConfig
	import PartitionsIndexLuceneRDD.ElementId
	import IDFUtils._
	
	// standard apply
	def apply[T](inputRDD: RDD[T],
	             numIndices: Int,
	             config: LuceneConfig = LuceneConfig.defaultConfig())
	            (implicit ct: ClassTag[T],
	             toIndexable: T => Indexable): DistributedIndexLuceneRDD[T] = {
		val numParts = inputRDD.partitions.length
		
		// build data RDD
		val elementsRDD = inputRDD.zipWithUniqueId().map(_.swap)
		
		// build indices
		val indicesRDD = elementsRDD.coalesce(numIndices)
			.mapPartitions(part => Iterator(createIndex(part, config, numParts))).setName("Indices")
		
		// create
		new DistributedIndexLuceneRDD[T](indicesRDD, elementsRDD)
	}
	
	// apply for Storeable data
	def apply[T](inputRDD: RDD[Storeable[T]],
	             numIndices: Int,
	             config: LuceneConfig, // FIXME default value breaks
	             dummy: String) // FIXME avoid needing summy to distinguish from other apply. implicit parameter?
	            (implicit ct: ClassTag[Storeable[T]],
	             ct2: ClassTag[T]): DistributedIndexLuceneRDD[T] = {
		val numParts = inputRDD.partitions.length
		
		// build keyed elements rdd
		val elementsRDD = inputRDD.zipWithUniqueId().map(_.swap)
		
		// build data RDD
		val dataRDD = elementsRDD.map(pair => (pair._1, pair._2.getData))
		
		// build indices
		val indicesRDD = elementsRDD.coalesce(numIndices)
			.mapPartitions(part => Iterator(createIndexStoreable(part, config, numParts))).setName("Indices")
		
		// create
		new DistributedIndexLuceneRDD[T](indicesRDD, dataRDD)
	}
	
	// apply for Storeable data with global IDF
	// warning: this will trigger index creation! as such, it caches the rdds already
	def fromStoreableWithGlobalIDF[T](inputRDD: RDD[Storeable[T]],
	                                  numIndices: Int,
	                                  config: LuceneConfig, // FIXME default value breaks
	                                  dummy: String) // FIXME avoid needing summy to distinguish from other apply. implicit parameter?
	                                 (implicit ct: ClassTag[Storeable[T]],
	                     ct2: ClassTag[T]): DistributedIndexLuceneRDD[T] = {
		val numParts = inputRDD.partitions.length
		
		// build keyed elements rdd
		val elementsRDD = inputRDD.zipWithUniqueId().map(_.swap)
		
		// build data RDD
		val dataRDD = elementsRDD.map(pair => (pair._1, pair._2.getData))
			.persist(StorageLevel.MEMORY_AND_DISK_SER)
		
		// build base indices
		val baseIndicesRDD = elementsRDD.coalesce(numIndices)
			.mapPartitions(part => Iterator(createIndexStoreable(part, config, numParts)))
			.setName("Indices")
			.persist(StorageLevel.MEMORY_AND_DISK_SER)
		
		// get document and term counts from base indices
		val docCounts = baseIndicesRDD.map(_.getDocumentCounts).treeReduce(combineDocumentCountsMaps)
		val termCounts = baseIndicesRDD.map(_.getTermCounts).treeReduce(combineTermCountsMaps)
		
		// calculate global idf from document and term counts
		val globalIdf: Map[String, Map[String, Float]] = calculateTermIDFs(docCounts, termCounts)
		
		// broadcast it
		val globalIdfBroadcast = inputRDD.sparkContext.broadcast(globalIdf)
		
		val indicesRDD: RDD[LuceneIndex] = baseIndicesRDD.map(index => GlobalIDFLuceneIndex(index.directory, index.config, globalIdfBroadcast))
		
		// create
		new DistributedIndexLuceneRDD[T](indicesRDD, dataRDD)
	}
	
	private def createIndex[T](zippedElements: Iterator[(Long, T)],
	                           config: LuceneConfig,
	                           numParts: Int)
	                          (implicit ct: ClassTag[T],
	                           toIndexable: T => Indexable)
	                          : LuceneIndex = {
		// create Lucene Directory and IndexWriter according to config
		val directory = new BigChunksRAMDirectory()
		val indexWriterConfig = buildIndexWriterConfig(config)
		val indexWriter = new IndexWriter(directory, indexWriterConfig)
		
		// index elements
		for (zippedElement <- zippedElements) {
			val (id, element) = zippedElement
			val document = indexable2LuceneDocument(element)
			document.add(new StoredField(ElementId, id))
			indexWriter.addDocument(document)
		}
		
		// compact index to one segment, commit & close
		indexWriter.getConfig.setUseCompoundFile(true)
		if (config.getCompactIndex) indexWriter.forceMerge(1, true)
		indexWriter.close()
		
		LuceneIndex(directory, config)
	}
	
	private def createIndexStoreable[T](zippedElements: Iterator[(Long, Storeable[T])],
	                           config: LuceneConfig,
	                           numParts: Int)
	                          (implicit ct: ClassTag[Storeable[T]],
	                           ct2: ClassTag[T])
	: LuceneIndex = {
		// create Lucene Directory and IndexWriter according to config
		val directory = new BigChunksRAMDirectory()
		val indexWriterConfig = buildIndexWriterConfig(config)
		val indexWriter = new IndexWriter(directory, indexWriterConfig)
		
		// index elements
		for (zippedElement <- zippedElements) {
			val (id, element) = zippedElement
			val document = indexable2LuceneDocument(element)
			document.add(new StoredField(ElementId, id))
			indexWriter.addDocument(document)
		}
		
		// compact index to one segment, commit & close
		indexWriter.getConfig.setUseCompoundFile(true)
		if (config.getCompactIndex) indexWriter.forceMerge(1, true)
		indexWriter.close()
		
		LuceneIndex(directory, config)
	}
}
