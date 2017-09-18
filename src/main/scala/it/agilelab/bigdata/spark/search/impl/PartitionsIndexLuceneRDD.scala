package it.agilelab.bigdata.spark.search.impl

import scala.reflect.ClassTag

import it.agilelab.bigdata.spark.search.SearchableRDD._
import it.agilelab.bigdata.spark.search.dsl.DslQuery
import it.agilelab.bigdata.spark.search.impl.LuceneIndexedPartition._
import it.agilelab.bigdata.spark.search.utils._
import it.agilelab.bigdata.spark.search.{RawQuery, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{OneToOneDependency, Partition, TaskContext}

/**
 * Implements SearchableRDD as an RDD of LuceneIndexedPartition
 */
class PartitionsIndexLuceneRDD[T] private[search](
    val indexedPartitionsRDD: RDD[LuceneIndexedPartition[T]],
    val config: LuceneConfig)
                                                 (implicit ct: ClassTag[T])
	extends SearchableRDD[T] (
		indexedPartitionsRDD.context,
		List(new OneToOneDependency(indexedPartitionsRDD))) {
	
	/*
		===============================================================================================
		===============================================================================================
			SearchableRDD methods implementations
		===============================================================================================
		===============================================================================================
	*/
	
	/** Returns the underlying RDD of indexed partitions.
		*/
	private def getIndexedPartitionsRDD = indexedPartitionsRDD.asInstanceOf[RDD[IndexedPartition[T]]] // FIXME: find a way to eliminate cast
	
	val numPartitions = getIndexedPartitionsRDD.partitions.length
	
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
		*
		* @see [[org.apache.spark.rdd.RDD#treeAggregate]]
		*
		* @group Search
		*/
	override def aggregatingSearch(query: Query,
	                      maxHits: Int,
	                      maxHitsPerIndex: Int = sameAsMaxHits)
	: Array[(T, Double)] = {
		val maxHitsPerIndexActual = getMHPIActual(maxHitsPerIndex, maxHits)
		val results = getIndexedPartitionsRDD.map(_.search(query, maxHitsPerIndexActual))
		aggregateResults(results, maxHits)
	}
	
	/** Like [[aggregatingSearchWithResultsTransformer(query:it\.agilelab\.bigdata\.spark\.search\.Query,maxHits:Int,maxHitsPerIndex:Int)* aggregatingSearch(Query,Int,Int)]],
		* but specifying a `transformer` function to be applied to the result elements.
		*
		* @group Search
		*/
	override def aggregatingSearchWithResultsTransformer[V](query: Query,
	                                                        maxHits: Int,
	                                                        resultsTransformer: T => V,
	                                                        maxHitsPerIndex: Int = sameAsMaxHits)
	: Array[(V, Double)] = {
		val maxHitsPerIndexActual = getMHPIActual(maxHitsPerIndex, maxHits)
		val results = getIndexedPartitionsRDD.map(_.search(query, maxHitsPerIndexActual, transformer))
		aggregateResults(results, maxHits)
	}
	
	/** Like [[aggregatingSearchWithResultsTransformer(query:it\.agilelab\.bigdata\.spark\.search\.Query,maxHits:Int,maxHitsPerIndex:Int)* aggregatingSearch(Query,Int,Int)]],
		* but returns the ids of the resulting elements.
		*
		* @note $ephemeralIDs
		* @group Search
		*/
	override def lightAggregatingSearch(query: Query,
	                           maxHits: Int,
	                           maxHitsPerIndex: Int = sameAsMaxHits)
	: Array[(Long, Double)] = {
		val maxHitsPerIndexActual = getMHPIActual(maxHitsPerIndex, maxHits)
		val results = getIndexedPartitionsRDD.map(_.lightSearch(query, maxHitsPerIndexActual))
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
		*
		* @group Search
		*/
	override def search(query: Query,
	           maxHits: Int,
	           maxHitsPerIndex: Int = sameAsMaxHits)
	: RDD[(T, Double)] = {
		val maxHitsPerIndexActual = getMHPIActual(maxHitsPerIndex, maxHits)
		val results = getIndexedPartitionsRDD.flatMap(_.search(query, maxHitsPerIndexActual))
		sortResults(results, maxHits)
	}
	
	/** Like [[searchWithResultsTransformer(query:it\.agilelab\.bigdata\.spark\.search\.Query,maxHits:Int,maxHitsPerIndex:Int)* search(Query,Int,Int)]],
		* but specifying a `transformer` function to be applied to the result elements.
		*
		* @group Search
		*/
	override def searchWithResultsTransformer[V](query: DslQuery,
	                                             maxHits: Int,
	                                             resultsTransformer: T => V,
	                                             maxHitsPerIndex: Int = sameAsMaxHits)
	: RDD[(V, Double)] = {
		val maxHitsPerIndexActual = getMHPIActual(maxHitsPerIndex, maxHits)
		val results = getIndexedPartitionsRDD.flatMap(_.search(query, maxHitsPerIndexActual, transformer))
		sortResults(results, maxHits)
	}
	
	/** Like [[searchWithResultsTransformer(query:it\.agilelab\.bigdata\.spark\.search\.Query,maxHits:Int,maxHitsPerIndex:Int)* search(Query,Int,Int)]],
		* but returns the ids of the resulting elements.
		*
		* @note $ephemeralIDs
		* @group Search
		*/
	override def lightSearch(query: Query,
	                maxHits: Int,
	                maxHitsPerIndex: Int = sameAsMaxHits)
	: RDD[(Long, Double)] = {
		val maxHitsPerIndexActual = getMHPIActual(maxHitsPerIndex, maxHits)
		val results = getIndexedPartitionsRDD.flatMap(_.lightSearch(query, maxHitsPerIndexActual))
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
		*
		* @group Search
		*/
	override def batchSearch(queries: Iterable[(Long, DslQuery)],
	                maxHits: Int,
	                maxHitsPerIndex: Int = sameAsMaxHits)
	: RDD[(Long, Array[(T, Double)])] = {
		val maxHitsPerIndexActual = getMHPIActual(maxHitsPerIndex, maxHits)
		val results = getIndexedPartitionsRDD.flatMap(_.batchSearch(queries.iterator, maxHitsPerIndexActual))
		aggregateResultsByKey(results, maxHits)
	}
	
	/** Like [[batchSearchWithResultsTransformer(queries:Iterator[(Long,it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery)],maxHits:Int,maxHitsPerIndex:Int)* batchSearch(Iterator[(Long,DslQuery)],Int,Int)]],
		* but specifying a `transformer` function to be applied to the result elements.
		*
		* @group Search
		*/
	override def batchSearchWithResultsTransformer[V](queries: Iterator[(Long, DslQuery)],
	                                                  maxHits: Int,
	                                                  resultsTransformer: T => V,
	                                                  maxHitsPerIndex: Int = sameAsMaxHits)
	: RDD[(Long, Array[(V, Double)])] = {
		val maxHitsPerIndexActual = getMHPIActual(maxHitsPerIndex, maxHits)
		val results = getIndexedPartitionsRDD.flatMap(_.batchSearch(queries, maxHitsPerIndexActual, transformer))
		aggregateResultsByKey(results, maxHits)
	}
	
	/** Like [[batchSearchWithResultsTransformer(queries:Iterator[(Long,it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery)],maxHits:Int,maxHitsPerIndex:Int)* batchSearch(Iterator[(Long,DslQuery)],Int,Int)]],
		* but returns the ids of the resulting elements.
		*
		* @note $ephemeralIDs
		* @group Search
		*/
	override def lightBatchSearch(queries: Iterator[(Long, DslQuery)],
	                     maxHits: Int,
	                     maxHitsPerIndex: Int = sameAsMaxHits)
	: RDD[(Long, Array[(Long, Double)])] = {
		val maxHitsPerIndexActual = getMHPIActual(maxHitsPerIndex, maxHits)
		val results = getIndexedPartitionsRDD.flatMap(_.lightBatchSearch(queries, maxHitsPerIndexActual))
		aggregateResultsByKey(results, maxHits)
	}
	
	/** Like [[batchSearchWithResultsTransformer(queries:Iterator[(Long,it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery)],maxHits:Int,maxHitsPerIndex:Int)* batchSearch(Iterator[(Long,DslQuery)],Int,Int)]],
		* but uses [[RawQuery]] type queries instead of [[dsl.DslQuery]].
		*
		* @group Search
		*/
	override def batchSearchRaw(queries: Iterator[(Long, RawQuery)],
	                   maxHits: Int,
	                   maxHitsPerIndex: Int = sameAsMaxHits)
	: RDD[(Long, Array[(T, Double)])] = {
		val maxHitsPerIndexActual = getMHPIActual(maxHitsPerIndex, maxHits)
		val results = getIndexedPartitionsRDD.flatMap(_.batchSearchRaw(queries, maxHitsPerIndexActual))
		aggregateResultsByKey(results, maxHits)
	}
	
	/** Like [[batchSearchWithResultsTransformer[V](queries:Iterator[(Long,it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery)],maxHits:Int,transformer:T=>V,maxHitsPerIndex:Int)* batchSearch(Iterator[(Long,DslQuery)],Int,T=>V,Int)]],
		* but uses [[RawQuery]] type queries instead of [[dsl.DslQuery]].
		*
		* @group Search
		*/
	override def batchSearchRawWithResultsTransformer[V](queries: Iterator[(Long, RawQuery)],
	                                                     maxHits: Int,
	                                                     resultsTransformer: T => V,
	                                                     maxHitsPerIndex: Int = sameAsMaxHits)
	: RDD[(Long, Array[(V, Double)])] = {
		val maxHitsPerIndexActual = getMHPIActual(maxHitsPerIndex, maxHits)
		val results = getIndexedPartitionsRDD.flatMap(_.batchSearchRaw(queries, maxHitsPerIndexActual, transformer))
		aggregateResultsByKey(results, maxHits)
	}
	
	/** Like [[lightBatchSearch(queries:Iterator[(Long,it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery)],maxHits:Int,maxHitsPerIndex:Int)* lightBatchSearch(Iterator[(Long,DslQuery)],Int,Int)]],
		* but uses [[RawQuery]] type queries instead of [[dsl.DslQuery]].
		*
		* @group Search
		*/
	override def lightBatchSearchRaw(queries: Iterator[(Long, RawQuery)],
	                        maxHits: Int,
	                        maxHitsPerIndex: Int = sameAsMaxHits)
	: RDD[(Long, Array[(Long, Double)])] = {
		val maxHitsPerIndexActual = getMHPIActual(maxHitsPerIndex, maxHits)
		val results = getIndexedPartitionsRDD.flatMap(_.lightBatchSearchRaw(queries, maxHitsPerIndexActual))
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
	override def queryJoin[U: ClassTag](other: RDD[U], queryGenerator: U => DslQuery, maxHits: Int): RDD[(U, Array[(T, Double)])] = {
		// assign id to elements
		val elementsWithId = other.zipWithUniqueId().map(_.swap)
		
		// create queries
		val queries = generateQueries(elementsWithId, queryGenerator)
		
		// do the search
		val results = queries.cartesian(this.getIndexedPartitionsRDD) map {
			case (queryBatch, indexedPartition) => indexedPartition.batchSearch(queryBatch, maxHits)
		}
		results.setName(f"Search results (cartesian) [${results.id}]")
		
		// process results
		val processedResults = processResults(results, maxHits)
		
		// combine results with elements originating the queries
		val combined = joinQueriesResults(elementsWithId, processedResults)
		
		combined
	}
	
	/** Like [[queryJoinWithResultsTransformer[U](other:org\.apache\.spark\.rdd\.RDD[U],queryGenerator:U=>it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery,maxHits:Int)* queryJoin[U](RDD[U],U=>DslQuery,Int)]],
		* but specifying a `transformer` function to be applied to the result elements.
		*
		* @group QueryJoin
		*/
	override def queryJoinWithResultsTransformer[U: ClassTag, V: ClassTag](other: RDD[U], queryGenerator: U => DslQuery, maxHits: Int, resultsTransformer: T => V): RDD[(U, Array[(V, Double)])] = {
		// assign id to elements
		val elementsWithId = other.zipWithUniqueId().map(_.swap)
		
		// create queries
		val queries = generateQueries(elementsWithId, queryGenerator)
		
		// do the search, process results
		val results = queries.cartesian(this.getIndexedPartitionsRDD) map {
			case (queryBatch, indexedPartition) => indexedPartition.batchSearch(queryBatch, maxHits, transformer)
		}
		results.setName(f"Search results (cartesian) [${results.id}]")
		
		// process results
		val processedResults = processResults(results, maxHits)
		
		// combine results with elements originating the queries
		val combined = joinQueriesResults(elementsWithId, processedResults)
		
		combined
	}
	
	/** Like [[queryJoinWithResultsTransformer[U](other:org\.apache\.spark\.rdd\.RDD[U],queryGenerator:U=>it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery,maxHits:Int)* queryJoin[U](RDD[U],U=>DslQuery,Int)]],
		* but returns the ids of the resulting elements.
		*
		* @note $ephemeralIDs
		* @group QueryJoin
		*/
	override def lightQueryJoin[U: ClassTag](other: RDD[U], queryGenerator: U => DslQuery, maxHits: Int): RDD[(U, Array[(Long, Double)])] = {
		// assign id to elements
		val elementsWithId = other.zipWithUniqueId().map(_.swap)
		
		// create query batches
		val queries = generateQueries(elementsWithId, queryGenerator)
		
		// do the search, process results
		val results = queries.cartesian(this.getIndexedPartitionsRDD) map {
			case (queryBatch, indexedPartition) => indexedPartition.lightBatchSearch(queryBatch, maxHits)
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
		val index = (id % numPartitions).toInt
		// function to process a partition to get the element back
		val process = (iterator: Iterator[IndexedPartition[T]]) => {
			if (iterator.hasNext) {
				// if there's elements in this partition, there's exactly one, so next().iterator is all we need
				val indexedPartition = iterator.next()
				// get the element
				indexedPartition.getElement(id)
			} else {
				// there's an empty partition, which should never happen; throw exception
				throw new NoSuchElementException("Empty partition: no IndexedPartition found in partition " + index +
					"while looking for id " + id + "!")
			}
		} : Option[T]
		// run a job against the partition containing id, return the first and only result
		val res = this.context.runJob[IndexedPartition[T], Option[T]](this.getIndexedPartitionsRDD, process, Array(index))
		res(0)
	}
	
	/** Like [[getElement(id:Long)* getElement(Long)]], but for an `Array` of ids. */
	override def getElements(ids: Array[Long]): Array[(Long, Option[T])] = {
		// find to which partition each id belongs
		// (see getElement above for the reasoning behind the calculation)
		val partitionIndexMap = ids.groupBy(id => (id % numPartitions).toInt)
		// function to process a partition to get back the elements belonging to it
		val process = (index: Int, iterator: Iterator[IndexedPartition[T]]) => {
			val partitionIds = partitionIndexMap.get(index).get
			if (iterator.hasNext) {
				// if there's elements in this partition, there's exactly one, so next().iterator is all we need
				val indexedPartition = iterator.next()
				// get the elements
				partitionIds map { id => (id, indexedPartition.getElement(id)) }
			} else {
				// there's an empty partition, which should never happen; throw exception
				throw new NoSuchElementException("Empty partition: no IndexedPartition found in partition " + index +
					"while looking for ids " + partitionIds.mkString("[",",","]") + "!")
			}
		} : Array[(Long, Option[T])]
		// map function to all partitions, collect resulting RDD
		val mappedRDD = this.getIndexedPartitionsRDD mapPartitionsWithIndex {
			case (index, iterator) => process(index, iterator).iterator
		}
		mappedRDD.collect()
	}
	
	override def getDocumentCounts: Map[String, Long] = indexedPartitionsRDD.map(_.partitionIndex.getDocumentCounts).treeReduce(combineDocumentCountsMaps)
	
	override def getTermCounts: Map[String, Map[String, Long]] = indexedPartitionsRDD.map(_.partitionIndex.getTermCounts).treeReduce(combineTermCountsMaps)
	
	override def getIndicesInfo: IndicesInfo = {
		val indexInfos = indexedPartitionsRDD.map(indexedPartition => indexedPartition.partitionIndex.getIndexInfo).collect().toList
		
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
	
	/**
	 * Returns the array of partitions.
	 *
	 * @return The array of partitions.
	 */
	override protected def getPartitions: Array[Partition] = getIndexedPartitionsRDD.partitions

	/** Overrides the compute function to return an iterator over the elements.
	 *  Returns iterators from the storage Arrays in each IndexedPartition.
	 */
	override def compute(split: Partition, context: TaskContext): Iterator[T] = {
		val splitIterator = firstParent[IndexedPartition[T]].iterator(split, context)
		if (splitIterator.hasNext) {
			// if there's elements in this partition, there's exactly one, so next().iterator is all we need
			splitIterator.next().iterator
		} else {
			// there's an empty partition, which should never happen; throw exception
			throw new NoSuchElementException("Empty partition: no IndexedPartition found in partition " + split.index + "!")
		}
	}

	/**
		* Applies the storage level to both the underlying RDD of IndexedPartition
		* and this RDD
		*
		* @param storageLevel
		* @return
		*/
	override def persist(storageLevel: StorageLevel) = {
		indexedPartitionsRDD.persist(storageLevel)
		
		this
	}
	
	override def unpersist(blocking: Boolean = true): PartitionsIndexLuceneRDD.this.type = {
		indexedPartitionsRDD.unpersist(blocking)
		
		this
	}

	/** Set the name for the RDD; By default set to "LuceneRDD" */
	override def setName(_name: String): this.type = {
		if (indexedPartitionsRDD.name != null) {
			indexedPartitionsRDD.setName(indexedPartitionsRDD.name + ", " + _name)
		} else {
			indexedPartitionsRDD.setName(_name)
		}
		this
		
	}
	setName(f"LuceneRDD [$id]") // set the name

	override def toDebugString(): String = {
		super.toDebugString
	}
	
	/*
	  ===============================================================================================
    ===============================================================================================
		  Other methods
    ===============================================================================================
	  ===============================================================================================
	 */
	
	def getConfigInfo: String = {
		"Configuration:\n" + config.getInfo
	}

	/**
		* Produces a string description of the number and size of data and indices of this LuceneRDD.
		* WARNING: this will cause the parent RDD to be materialized and indexed!
		*
		* @return a string describing this RDD
		*/
	def getDataAndIndicesInfo: String = {
		val indicesInfo = indexedPartitionsRDD mapPartitionsWithIndex {
			case (part, it) => Iterator((part, it.map(indexedPartition => indexedPartition.getIndexedPartitionInfo).toArray))
		}

		val partitionInfo = indicesInfo.collect() map {
			case (partitionNumber, indexInfoIterator) => {
				val partitionHeader = f"Partition $partitionNumber:\n"
				val indicesInfo = indexInfoIterator.zipWithIndex.map {
					case (indexInfo, indexNumber) => {
						val indexHeader = f"\t[LuceneIndexedPartition $indexNumber] "
						indexHeader + indexInfo
					}
				}
				partitionHeader + indicesInfo.mkString("\n")
			}
		}

		partitionInfo.mkString("\n")
	}
}

object PartitionsIndexLuceneRDD {
	final val SpecialFieldMarker = '$' // fields starting with this are special
	final val ElementId = SpecialFieldMarker + "id"

	/**
	  * Converts an input RDD into a LuceneRDD.
		* For each partition fo the input RDD, an index is created.
		* Thus, the new RDD will have a number of elements equal to the number of
		* partitions of the input RDD, each in its own partition.
	  *
	  * @param inputRDD The input RDD.
	  * @return The LuceneRDD.
	  */
	def apply[T](
		  inputRDD: RDD[T],
			config: LuceneConfig = LuceneConfig.defaultConfig())
	   (implicit ct: ClassTag[T],
	    toIndexable: T => Indexable)
	   : PartitionsIndexLuceneRDD[T] = {
		val numParts = inputRDD.partitions.length
		// create an indexed partition for each partition of the input rdd
		val luceneIndexedPartitions = inputRDD
			.zipWithUniqueId()
			.mapPartitions(part => Iterator(createLuceneIndexedPartition(part, config, numParts))).setName("IndexedPartitions")
		new PartitionsIndexLuceneRDD[T](luceneIndexedPartitions, config)
	}
	
	/**
		* Converts an input RDD into a LuceneRDD.
		* For each partition fo the input RDD, an index is created.
		* Thus, the new RDD will have a number of elements equal to the number of
		* partitions of the input RDD, each in its own partition.
		*
		* @param inputRDD The input RDD.
		* @return The LuceneRDD.
		*/
	def apply[T](inputRDD: RDD[Storeable[T]],
		           config: LuceneConfig, // FIXME default value breaks
	             dummy: String) // FIXME avoid needing summy to distinguish from other apply. implicit parameter?
	            (implicit ct: ClassTag[Storeable[T]],
	             ct2: ClassTag[T])
	: PartitionsIndexLuceneRDD[T] = {
		val numParts = inputRDD.partitions.length
		// create an indexed partition for each partition of the input rdd
		val luceneIndexedPartitions = inputRDD
			.zipWithUniqueId()
			.mapPartitions(part => Iterator(createLuceneIndexedPartitionStoreable(part, config, numParts))).setName("IndexedPartitions")
		new PartitionsIndexLuceneRDD[T](luceneIndexedPartitions, config)
	}
}