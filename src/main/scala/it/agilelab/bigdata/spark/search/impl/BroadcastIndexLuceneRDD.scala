package it.agilelab.bigdata.spark.search.impl

import scala.reflect.ClassTag

import it.agilelab.bigdata.spark.search._
import it.agilelab.bigdata.spark.search.dsl.DslQuery
import it.agilelab.bigdata.spark.search.utils._
import org.apache.lucene.document.StoredField
import org.apache.lucene.index.IndexWriter
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{OneToOneDependency, Partition, TaskContext}

class BroadcastIndexLuceneRDD[T] private[search] (
  val index: Broadcast[LuceneIndex],
	val elementsRDD: RDD[Array[T]])
	(implicit ct: ClassTag[T])
	extends SearchableRDD[T] (
		elementsRDD.context,
		List(new OneToOneDependency(elementsRDD))) {
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
		val numPartitions = elementsRDD.getNumPartitions
		val results = elementsRDD mapPartitionsWithIndex {
			case (partitionIndex, partition) =>
				// get the index from broadcast variable, search it
				val realIndex = index.value
				val results = realIndex.searchAny(query, maxHits)
				// filter elements in this partition
				val resultsInThisPartition = results filter {
					case (elementId, score) => (elementId % numPartitions).toInt == partitionIndex // cast is safe thanks to elementId's construction
				}
				// map elementIds into elements
				val elements = partition.next() // there's always exactly one array per partition
				resultsInThisPartition map {
					case (elementId, score) =>
						val elementIndex = (elementId / numPartitions).toInt // cast is safe thanks to elementId's construction
						(elements(elementIndex), score)
				}
		}
		val iteratorResults = results.mapPartitions(it => Iterator(it))
		aggregateResults(iteratorResults, maxHits)
	}
	
	/** Like [[aggregatingSearch(query:it\.agilelab\.bigdata\.spark\.search\.Query,maxHits:Int,maxHitsPerIndex:Int)* aggregatingSearch(Query,Int,Int)]],
		* but specifying a `transformer` function to be applied to the result elements.
		*
		* @group Search
		*/
	override def aggregatingSearch[V](query: Query, maxHits: Int, transformer: (T) => V, maxHitsPerIndex: Int): Array[(V, Double)] = {
		val numPartitions = elementsRDD.getNumPartitions
		val results = elementsRDD mapPartitionsWithIndex {
			case (partitionIndex, partition) =>
				// get the index from broadcast variable, search it
				val realIndex = index.value
				val results = realIndex.searchAny(query, maxHits)
				// filter elements in this partition
				val resultsInThisPartition = results filter {
					case (elementId, score) => (elementId % numPartitions).toInt == partitionIndex // cast is safe thanks to elementId's construction
				}
				// map elementIds into elements
				val elements = partition.next() // there's always exactly one array per partition
				resultsInThisPartition map {
					case (elementId, score) =>
						val elementIndex = (elementId / numPartitions).toInt // cast is safe thanks to elementId's construction
						(transformer(elements(elementIndex)), score)
				}
		}
		val iteratorResults = results.mapPartitions(it => Iterator(it))
		aggregateResults(iteratorResults, maxHits)
	}
	
	/** Like [[aggregatingSearch(query:it\.agilelab\.bigdata\.spark\.search\.Query,maxHits:Int,maxHitsPerIndex:Int)* aggregatingSearch(Query,Int,Int)]],
		* but returns the ids of the resulting elements.
		*
		* @note $ephemeralIDs
		* @group Search
		*/
	override def lightAggregatingSearch(query: Query, maxHits: Int, maxHitsPerIndex: Int): Array[(Long, Double)] = {
		index.value.searchAny(query, maxHits).toArray
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
		val numPartitions = elementsRDD.getNumPartitions
		val results = elementsRDD mapPartitionsWithIndex {
			case (partitionIndex, partition) =>
				// get the index from broadcast variable, search it
				val realIndex = index.value
				val results = realIndex.searchAny(query, maxHits)
				// filter elements in this partition
				val resultsInThisPartition = results filter {
					case (elementId, score) => (elementId % numPartitions).toInt == partitionIndex // cast is safe thanks to elementId's construction
				}
				// map elementIds into elements
				val elements = partition.next() // there's always exactly one array per partition
				resultsInThisPartition map {
					case (elementId, score) =>
						val elementIndex = (elementId / numPartitions).toInt // cast is safe thanks to elementId's construction
						(elements(elementIndex), score)
				}
		}
		sortResults(results, maxHits)
	}
	
	/** Like [[search(query:it\.agilelab\.bigdata\.spark\.search\.Query,maxHits:Int,maxHitsPerIndex:Int)* search(Query,Int,Int)]],
		* but specifying a `transformer` function to be applied to the result elements.
		*
		* @group Search
		*/
	override def search[V](query: DslQuery, maxHits: Int, transformer: (T) => V, maxHitsPerIndex: Int): RDD[(V, Double)] = {
		val numPartitions = elementsRDD.getNumPartitions
		val results = elementsRDD mapPartitionsWithIndex {
			case (partitionIndex, partition) =>
				// get the index from broadcast variable, search it
				val realIndex = index.value
				val results = realIndex.searchAny(query, maxHits)
				// filter elements in this partition
				val resultsInThisPartition = results filter {
					case (elementId, score) => (elementId % numPartitions).toInt == partitionIndex // cast is safe thanks to elementId's construction
				}
				// map elementIds into elements
				val elements = partition.next() // there's always exactly one array per partition
				resultsInThisPartition map {
					case (elementId, score) =>
						val elementIndex = (elementId / numPartitions).toInt // cast is safe thanks to elementId's construction
						(transformer(elements(elementIndex)), score)
				}
		}
		sortResults(results, maxHits)
	}
	
	/** Like [[search(query:it\.agilelab\.bigdata\.spark\.search\.Query,maxHits:Int,maxHitsPerIndex:Int)* search(Query,Int,Int)]],
		* but returns the ids of the resulting elements.
		*
		* @note $ephemeralIDs
		* @group Search
		*/
	override def lightSearch(query: Query, maxHits: Int, maxHitsPerIndex: Int): RDD[(Long, Double)] = {
		// FIXME this is wasteful, but it's the only easy way to obtain an RDD of element ids...
		val numPartitions = elementsRDD.getNumPartitions
		val results = elementsRDD mapPartitionsWithIndex {
			case (partitionIndex, partition) =>
				// get the index from broadcast variable, search it
				val realIndex = index.value
				val results = realIndex.searchAny(query, maxHits)
				// filter elements in this partition
				val resultsInThisPartition = results filter {
					case (elementId, score) => (elementId % numPartitions).toInt == partitionIndex // cast is safe thanks to elementId's construction
				}
				resultsInThisPartition
		}
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
	override def batchSearch(queries: Iterable[(Long, DslQuery)],
	                         maxHits: Int,
	                         maxHitsPerIndex: Int)
	                        : RDD[(Long, Array[(T, Double)])] = {
		val numPartitions = elementsRDD.getNumPartitions
		val results = elementsRDD mapPartitionsWithIndex {
			case (partitionIndex, partition) =>
				// get the index from broadcast variable, run queries against it
				val realIndex = index.value
				val results = queries map {
					case (queryId, query) => (queryId, realIndex.searchAny(query, maxHits))
				}
				// filter elements in this partition
				val resultsInThisPartition = results map {
					case (queryId, queryResults) =>
						val queryResultsInThisPartition = queryResults filter {
							case (elementId, score) => (elementId % numPartitions).toInt == partitionIndex // cast is safe thanks to elementId's construction
						}
						
						(queryId,queryResultsInThisPartition)
				}
				// map elementIds into elements
				val elements = partition.next() // there's always exactly one array per partition
				val elementResults = resultsInThisPartition map {
					case (queryId, queryResults) =>
						val queryElementResults = queryResults map {
							case (elementId, score) =>
								val elementIndex = (elementId / numPartitions).toInt // cast is safe thanks to elementId's construction
								(elements(elementIndex), score)
						}
						
						(queryId, queryElementResults)
				}
				
				elementResults.iterator
		}
		aggregateResultsByKey(results, maxHits)
	}
	
	/** Like [[batchSearch(queries:Iterator[(Long,it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery)],maxHits:Int,maxHitsPerIndex:Int)* batchSearch(Iterator[(Long,DslQuery)],Int,Int)]],
		* but specifying a `transformer` function to be applied to the result elements.
		*
		* @group Search
		*/
	override def batchSearch[V](queries: Iterator[(Long, DslQuery)], maxHits: Int, transformer: (T) => V, maxHitsPerIndex: Int): RDD[(Long, Array[(V, Double)])] = {
		val numPartitions = elementsRDD.getNumPartitions
		val results = elementsRDD mapPartitionsWithIndex {
			case (partitionIndex, partition) =>
				// get the index from broadcast variable, run queries against it
				val realIndex = index.value
				val results = queries map {
					case (queryId, query) => (queryId, realIndex.searchAny(query, maxHits))
				}
				// filter elements in this partition
				val resultsInThisPartition = results map {
					case (queryId, queryResults) =>
						val queryResultsInThisPartition = queryResults filter {
							case (elementId, score) => (elementId % numPartitions).toInt == partitionIndex // cast is safe thanks to elementId's construction
						}
						
						(queryId,queryResultsInThisPartition)
				}
				// map elementIds into elements
				val elements = partition.next() // there's always exactly one array per partition
				val elementResults = resultsInThisPartition map {
					case (queryId, queryResults) =>
						val queryElementResults = queryResults map {
							case (elementId, score) =>
								val elementIndex = (elementId / numPartitions).toInt // cast is safe thanks to elementId's construction
								(transformer(elements(elementIndex)), score)
						}
						
						(queryId, queryElementResults)
				}
				
				elementResults
		}
		aggregateResultsByKey(results, maxHits)
	}
	
	/** Like [[batchSearch(queries:Iterator[(Long,it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery)],maxHits:Int,maxHitsPerIndex:Int)* batchSearch(Iterator[(Long,DslQuery)],Int,Int)]],
		* but returns the ids of the resulting elements.
		*
		* @note $ephemeralIDs
		* @group Search
		*/
	override def lightBatchSearch(queries: Iterator[(Long, DslQuery)], maxHits: Int, maxHitsPerIndex: Int): RDD[(Long, Array[(Long, Double)])] = {
		// FIXME this is wasteful, but it's the only easy way to obtain an RDD of element ids...
		val numPartitions = elementsRDD.getNumPartitions
		val results = elementsRDD mapPartitionsWithIndex {
			case (partitionIndex, partition) =>
				// get the index from broadcast variable, run queries against it
				val realIndex = index.value
				val results = queries map {
					case (queryId, query) => (queryId, realIndex.searchAny(query, maxHits))
				}
				// filter elements in this partition
				val resultsInThisPartition = results map {
					case (queryId, queryResults) =>
						val queryResultsInThisPartition = queryResults filter {
							case (elementId, score) => (elementId % numPartitions).toInt == partitionIndex // cast is safe thanks to elementId's construction
						}
						
						(queryId,queryResultsInThisPartition)
				}
				resultsInThisPartition
		}
		aggregateResultsByKey(results, maxHits)
	}
	
	/** Like [[batchSearch(queries:Iterator[(Long,it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery)],maxHits:Int,maxHitsPerIndex:Int)* batchSearch(Iterator[(Long,DslQuery)],Int,Int)]],
		* but uses [[RawQuery]] type queries instead of [[dsl.DslQuery]].
		*
		* @group Search
		*/
	override def batchSearchRaw(queries: Iterator[(Long, RawQuery)], maxHits: Int, maxHitsPerIndex: Int): RDD[(Long, Array[(T, Double)])] = {
		val numPartitions = elementsRDD.getNumPartitions
		val results = elementsRDD mapPartitionsWithIndex {
			case (partitionIndex, partition) =>
				// get the index from broadcast variable, run queries against it
				val realIndex = index.value
				val results = queries map {
					case (queryId, query) => (queryId, realIndex.searchAny(query, maxHits))
				}
				// filter elements in this partition
				val resultsInThisPartition = results map {
					case (queryId, queryResults) =>
						val queryResultsInThisPartition = queryResults filter {
							case (elementId, score) => (elementId % numPartitions).toInt == partitionIndex // cast is safe thanks to elementId's construction
						}
						
						(queryId,queryResultsInThisPartition)
				}
				// map elementIds into elements
				val elements = partition.next() // there's always exactly one array per partition
				val elementResults = resultsInThisPartition map {
					case (queryId, queryResults) =>
						val queryElementResults = queryResults map {
							case (elementId, score) =>
								val elementIndex = (elementId / numPartitions).toInt // cast is safe thanks to elementId's construction
								(elements(elementIndex), score)
						}
						
						(queryId, queryElementResults)
				}
				
				elementResults
		}
		aggregateResultsByKey(results, maxHits)
	}
	
	/** Like [[batchSearch[V](queries:Iterator[(Long,it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery)],maxHits:Int,transformer:T=>V,maxHitsPerIndex:Int)* batchSearch(Iterator[(Long,DslQuery)],Int,T=>V,Int)]],
		* but uses [[RawQuery]] type queries instead of [[dsl.DslQuery]].
		*
		* @group Search
		*/
	override def batchSearchRaw[V](queries: Iterator[(Long, RawQuery)], maxHits: Int, transformer: (T) => V, maxHitsPerIndex: Int): RDD[(Long, Array[(V, Double)])] = {
		val numPartitions = elementsRDD.getNumPartitions
		val results = elementsRDD mapPartitionsWithIndex {
			case (partitionIndex, partition) =>
				// get the index from broadcast variable, run queries against it
				val realIndex = index.value
				val results = queries map {
					case (queryId, query) => (queryId, realIndex.searchAny(query, maxHits))
				}
				// filter elements in this partition
				val resultsInThisPartition = results map {
					case (queryId, queryResults) =>
						val queryResultsInThisPartition = queryResults filter {
							case (elementId, score) => (elementId % numPartitions).toInt == partitionIndex // cast is safe thanks to elementId's construction
						}
						
						(queryId,queryResultsInThisPartition)
				}
				// map elementIds into elements
				val elements = partition.next() // there's always exactly one array per partition
				val elementResults = resultsInThisPartition map {
					case (queryId, queryResults) =>
						val queryElementResults = queryResults map {
							case (elementId, score) =>
								val elementIndex = (elementId / numPartitions).toInt // cast is safe thanks to elementId's construction
								(transformer(elements(elementIndex)), score)
						}
						
						(queryId, queryElementResults)
				}
				
				elementResults
		}
		aggregateResultsByKey(results, maxHits)
	}
	
	/** Like [[lightBatchSearch(queries:Iterator[(Long,it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery)],maxHits:Int,maxHitsPerIndex:Int)* lightBatchSearch(Iterator[(Long,DslQuery)],Int,Int)]],
		* but uses [[RawQuery]] type queries instead of [[dsl.DslQuery]].
		*
		* @group Search
		*/
	override def lightBatchSearchRaw(queries: Iterator[(Long, RawQuery)], maxHits: Int, maxHitsPerIndex: Int): RDD[(Long, Array[(Long, Double)])] = {
		// FIXME this is wasteful, but it's the only easy way to obtain an RDD of element ids...
		val numPartitions = elementsRDD.getNumPartitions
		val results = elementsRDD mapPartitionsWithIndex {
			case (partitionIndex, partition) =>
				// get the index from broadcast variable, run queries against it
				val realIndex = index.value
				val results = queries map {
					case (queryId, query) => (queryId, realIndex.searchAny(query, maxHits))
				}
				// filter elements in this partition
				val resultsInThisPartition = results map {
					case (queryId, queryResults) =>
						val queryResultsInThisPartition = queryResults filter {
							case (elementId, score) => (elementId % numPartitions).toInt == partitionIndex // cast is safe thanks to elementId's construction
						}
						
						(queryId,queryResultsInThisPartition)
				}
				resultsInThisPartition
		}
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
	override def queryJoin[U: ClassManifest](other: RDD[U], queryGenerator: (U) => DslQuery, maxHits: Int): RDD[(U, Array[(T, Double)])] = ???
	
	/** Like [[queryJoin[U](other:org\.apache\.spark\.rdd\.RDD[U],queryGenerator:U=>it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery,maxHits:Int)* queryJoin[U](RDD[U],U=>DslQuery,Int)]],
		* but specifying a `transformer` function to be applied to the result elements.
		*
		* @group QueryJoin
		*/
	override def queryJoin[U: ClassManifest, V: ClassManifest](other: RDD[U], queryGenerator: (U) => DslQuery, maxHits: Int, transformer: (T) => V): RDD[(U, Array[(V, Double)])] = ???
	
	/** Like [[queryJoin[U](other:org\.apache\.spark\.rdd\.RDD[U],queryGenerator:U=>it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery,maxHits:Int)* queryJoin[U](RDD[U],U=>DslQuery,Int)]],
		* but returns the ids of the resulting elements.
		*
		* @note $ephemeralIDs
		* @group QueryJoin
		*/
	override def lightQueryJoin[U : ClassManifest](other: RDD[U], queryGenerator: (U) => DslQuery, maxHits: Int): RDD[(U, Array[(Long, Double)])] = ???
	
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
		val process = (iterator: Iterator[Array[T]]) => {
			if (iterator.hasNext) {
				// if there's elements in this partition, there's exactly one, so next().iterator is all we need
				val array = iterator.next()
				// get the element
				val elementIndex = (id / elementsRDD.getNumPartitions).toInt // cast is safe thanks to elementId's construction
				Some(array(elementIndex))
			} else {
				// there's an empty partition, which should never happen; throw exception
				throw new NoSuchElementException("Empty partition: no IndexedPartition found in partition " + index +
					"while looking for id " + id + "!")
			}
		} : Option[T]
		// run a job against the partition containing id, return the first and only result
		val res = this.context.runJob[Array[T], Option[T]](elementsRDD, process, Array(index))
		res(0)
	}
	
	/** Like [[getElement(id:Long)* getElement(Long)]], but for an `Array` of ids. */
	override def getElements(ids: Array[Long]): Array[(Long, Option[T])] = {
		// get a set of ids for fast matching
		val idSet = ids.toSet
		// the ids are generated with zipWithUniqueIds, therefore we
		// can easily find out the partition to which id belongs by
		// calculating id modulo number of partitions
		val numPartitions = elementsRDD.getNumPartitions
		val partitionIndices = idSet.map(_ % numPartitions).toArray
		
		val results = elementsRDD.mapPartitionsWithIndex {
			case (partitionIndex, elements) => {
				// get the subset of ids that are in this partition
				val partitionIdSet = idSet.filter(_ % numPartitions == partitionIndex)
				// there's exactly one array per partition
				val array = elements.next()
				// get the elements that actually exist and the ids of those that don't
				val (existent, nonExistent) = partitionIdSet.partition(elementId => elementId / numPartitions < array.length)
				val results = existent.iterator.map(elementId => (elementId, array((elementId / numPartitions).toInt))) // toInt is safe because of how elementId is constructed
				// map results to some, the rest to none
				results.map { case (elId, el) => (elId, Some(el)) } ++ nonExistent.map((_, None))
			}
		}
		results.collect()
	}
	
	override def getDocumentCounts: Map[String, Long] = index.value.getDocumentCounts
	
	override def getTermCounts: Map[String, Map[String, Long]] = index.value.getTermCounts
	
	override def getTermIDFs: Map[String, Map[String, Float]] = index.value.getTermIDFs
	
	override def getIndicesInfo: IndicesInfo = {
		val indexInfo = index.value.getIndexInfo
		
		val totDocs = indexInfo.numDocuments.toLong
		val totBytes = indexInfo.sizeBytes
		
		IndicesInfo(1, totDocs, totBytes, List(indexInfo))
	}
	
	/*
	  ===============================================================================================
    ===============================================================================================
	  	RDD methods implementations
    ===============================================================================================
	  ===============================================================================================
	 */
	
	@DeveloperApi
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
	
	override protected def getPartitions: Array[Partition] = elementsRDD.partitions
	
	override def persist(newLevel: StorageLevel): BroadcastIndexLuceneRDD.this.type = {
		elementsRDD.persist(newLevel)
		
		this
	}
	
	override def unpersist(blocking: Boolean = true): BroadcastIndexLuceneRDD.this.type = {
		elementsRDD.unpersist(blocking)
		index.unpersist(blocking)
		
		this
	}
}
object BroadcastIndexLuceneRDD {
	import LuceneIndexedPartition.indexable2LuceneDocument
	import LuceneIndexedPartition.buildIndexWriterConfig
	import PartitionsIndexLuceneRDD.ElementId
	
	def apply[T](inputRDD: RDD[T],
	             config: LuceneConfig = LuceneConfig.defaultConfig())
	            (implicit ct: ClassTag[T],
	             toIndexable: T => Indexable): BroadcastIndexLuceneRDD[T] = {
		val numParts = inputRDD.partitions.length
		
		// build data RDD
		val dataRDD = inputRDD.zipWithUniqueId()
		
		// build indices
		val indicesRDD = dataRDD.mapPartitions(part => Iterator(createIndex(part, config, numParts))).setName("Indices")
		
		// treeaggregate indices
		val redOp = (index1: BigChunksRAMDirectory, index2: BigChunksRAMDirectory) => {
			// prepare new index/directory
			val newDirectory = new BigChunksRAMDirectory()
			val indexWriterConfig = buildIndexWriterConfig(config)
			val indexWriter = new IndexWriter(newDirectory, indexWriterConfig)
			
			// add old indices
			indexWriter.addIndexes(index1, index2)
			
			// compact index to one segment, commit & close
			indexWriter.getConfig.setUseCompoundFile(true)
			if (config.getCompactIndex) indexWriter.forceMerge(1, true)
			indexWriter.close()
			
			// return new directory
			newDirectory
		}
		val indexDirectory = indicesRDD.treeReduce(redOp)
		
		// create index
		val index = LuceneIndex(indexDirectory, config)
		
		// broadcast index
		val sc = indicesRDD.sparkContext
		val broadcastIndex = sc.broadcast(index)
		
		// create elements[] RDD
		val elementsRDD = dataRDD.map(_._1).glom()
		
		// create
		new BroadcastIndexLuceneRDD[T](broadcastIndex, elementsRDD)
	}
	
	private def createIndex[T](zippedElements: Iterator[(T, Long)],
		                      config: LuceneConfig,
		                      numParts: Int)
	                       (implicit ct: ClassTag[T],
	                        toIndexable: T => Indexable) = {
		// create Lucene Directory and IndexWriter according to config
		val directory = new BigChunksRAMDirectory()
		val indexWriterConfig = buildIndexWriterConfig(config)
		val indexWriter = new IndexWriter(directory, indexWriterConfig)
		
		// index elements
		for (zippedElement <- zippedElements) {
			val (element, id) = zippedElement
			val document = indexable2LuceneDocument(element)
			document.add(new StoredField(ElementId, id))
			indexWriter.addDocument(document)
		}
		
		// compact index to one segment, commit & close
		indexWriter.getConfig.setUseCompoundFile(true)
		if (config.getCompactIndex) indexWriter.forceMerge(1, true)
		indexWriter.close()
		
		directory
	}
}