package it.agilelab.bigdata.spark.search

import it.agilelab.bigdata.spark.search.dsl.DslQuery
import it.agilelab.bigdata.spark.search.utils._
import org.apache.spark.{Dependency, SparkContext}
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag


/** Indexed RDD that provides search functionality.
	*
	* ==Principle of operation==
	*
	* The elements of the input RDD must implement the [[Indexable]] trait; an advanced mechanism
	* based on implicit conversion using code generation is available that supports common data
	* types and classes built from them, so no work at all is required in many cases; see [[Indexable]]
	* for details.
	*
	* For each partition of the input RDD, an [[IndexedPartition]] is built, which contains both the
	* [[Index]] for the data in the partition and the data itself.
	*
	* Thus, the number of indices built is the same as the number of partitions of the input RDD; every
	* operation must interrogate all the indices, so the number of indices directly affects performance.
	*
	* This means a compromise must be made between efficient operation and partition number/size; this
	* is an important concern as big partitions can easily wreak havoc and cause OOM errors, especially
	* during shuffles.
	*
	* Work is underway to decouple the data partitioning from the indexes using different RDDs and/or
	* broadcast variables, in order to alleviate this problem.
	*
	* ==Queries==
	*
	* The queries can either be of type [[dsl.DslQuery DslQuery]] or of type [[RawQuery]]. In the latter
	* case the way they are interpreted is implementation-dependent.
	*
	* ==Functionalities==
	*
	* The functionalities provided are broadly separable in two categories:
	*  - '''search'''
	*  - '''query join'''
	*
	* '''Search''' functionalities are implemented with methods with `search` in their name, and are
	* of two kinds:
	*  - those that return results directly, similar to a `collect()`
	*  - those that return RDDs
	*
	* Obviously, the latter ones are much more suited when many queries are to be executed or when results
	* contain many and/or large elements.
	*
	* '''Query join''' functionalities allow the execution of a join operation on an RDD and a
	* SearchableRDD, in which the join predicate is a query which is built from each of the elements
	* of the other RDD and executed on the SearchableRDD.
	* The other RDD can be the SearchableRDD itself.
	*
	* ==Variations==
	*
	* For methods of both functionalities, two variations are available:
	*  - '''light'''
	*  - '''transformer''' which returns the result of applying the provided transformer function to the results
	*
	* '''Light''' variations are indicated by a `light` prefix to the corresponding base method. They
	* return `Long` ids instead of elements as results; this means that results use much less memory.
	*
	* '''Transformer''' variations specify an additional transformer function which is applied to the
	* elements; this allows the extraction of the relevant information from reults to be done as soon
	* as possible, again helping with memory usage.
	*
	* @note $consistentRDD
	*
	* @see [[org.apache.spark.rdd.RDD#zipWithUniqueId]]
	*
	* @tparam T the type of the elements.
	*
	* @define consistentRDD  Some operations use zipWithUniqueId internally; as such, the same caveats
	*                        apply, namely that the assigned id is order-dependent. This means that
	*                        '''the input RDD must be consistent upon reevaluation''', that is, the
	*                        elements inside the partitions and their ordering must be guaranteed to not
	*                        change between reevaluations. Otherwise, the behaviour is undefined.
	*
	* @define ephemeralIDs These ids are ephemeral, and can change between instances of this RDD;
	*                      they are only meaningful when used in combination with the same RDD that
	*                      provided them.
	*
	* @define intermediateResults Setting the maximum number of intermediate results to be lower than
	*                             the maximum number of results can lead to good results not being
	*                             returned, especially with non-uniform data distributions.
	* @groupdesc Search These operations provide search functionality.
	* @groupprio Search 1
	* @groupname QueryJoin Query Join
	* @groupdesc QueryJoin  These operations provide query join functionality.
	* @groupprio QueryJoin 2
	*/
abstract class SearchableRDD[T](
    @transient private var _sc: SparkContext,
    @transient private var deps: Seq[Dependency[_]])
   (implicit ct: ClassTag[T])
	extends RDD[T](_sc, deps) {
	import SearchableRDD.sameAsMaxHits
	import IDFUtils.calculateTermIDFs

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
	def aggregatingSearch(query: Query,
	                      maxHits: Int,
	                      maxHitsPerIndex: Int = sameAsMaxHits)
	                     : Array[(T, Double)]

	/** Like [[aggregatingSearchWithResultsTransformer(query:it\.agilelab\.bigdata\.spark\.search\.Query,maxHits:Int,maxHitsPerIndex:Int)* aggregatingSearch(Query,Int,Int)]],
		* but specifying a `transformer` function to be applied to the result elements.
		*
		* @group Search
		*/
	def aggregatingSearchWithResultsTransformer[V](query: Query,
	                                               maxHits: Int,
	                                               resultsTransformer: T => V,
	                                               maxHitsPerIndex: Int = sameAsMaxHits)
	                                       : Array[(V, Double)]

	/** Like [[aggregatingSearchWithResultsTransformer(query:it\.agilelab\.bigdata\.spark\.search\.Query,maxHits:Int,maxHitsPerIndex:Int)* aggregatingSearch(Query,Int,Int)]],
		* but returns the ids of the resulting elements.
		*
		* @note $ephemeralIDs
		* @group Search
		*/
	def lightAggregatingSearch(query: Query,
	                           maxHits: Int,
	                           maxHitsPerIndex: Int = sameAsMaxHits)
	                          : Array[(Long, Double)]

	/** Returns an `RDD` of elements matching the `query`, returning at most `maxHits` results.
	  *
	  * Setting `maxHitsPerIndex` limits the size of intermediate results. It must be a positive value
	  * between `1` and `maxHits` (both inclusive). The (internally handled) default is `maxHits`.
	  *
	  * @note $intermediateResults
		*
		* @group Search
	  */
	def search(query: Query,
	           maxHits: Int,
	           maxHitsPerIndex: Int = sameAsMaxHits)
	          : RDD[(T, Double)]

	/** Like [[searchWithResultsTransformer(query:it\.agilelab\.bigdata\.spark\.search\.Query,maxHits:Int,maxHitsPerIndex:Int)* search(Query,Int,Int)]],
	  * but specifying a `transformer` function to be applied to the result elements.
		*
		* @group Search
	  */
	def searchWithResultsTransformer[V](query: DslQuery,
	                                    maxHits: Int,
	                                    resultsTransformer: T => V,
	                                    maxHitsPerIndex: Int = sameAsMaxHits)
	                                   : RDD[(V, Double)]

	/** Like [[searchWithResultsTransformer(query:it\.agilelab\.bigdata\.spark\.search\.Query,maxHits:Int,maxHitsPerIndex:Int)* search(Query,Int,Int)]],
	  * but returns the ids of the resulting elements.
	  *
	  * @note $ephemeralIDs
		* @group Search
	  */
	def lightSearch(query: Query,
	                maxHits: Int,
	                maxHitsPerIndex: Int = sameAsMaxHits)
	               : RDD[(Long, Double)]

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
	def batchSearch(queries: Iterable[(Long, DslQuery)],
	                maxHits: Int,
	                maxHitsPerIndex: Int = sameAsMaxHits)
	               : RDD[(Long, Array[(T, Double)])]

	/** Like [[batchSearchWithResultsTransformer(queries:Iterator[(Long,it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery)],maxHits:Int,maxHitsPerIndex:Int)* batchSearch(Iterator[(Long,DslQuery)],Int,Int)]],
	  * but specifying a `transformer` function to be applied to the result elements.
		*
		* @group Search
	  */
	def batchSearchWithResultsTransformer[V](queries: Iterator[(Long, DslQuery)],
	                                         maxHits: Int,
	                                         resultsTransformer: T => V,
	                                         maxHitsPerIndex: Int = sameAsMaxHits)
	                                        : RDD[(Long, Array[(V, Double)])]

	/** Like [[batchSearchWithResultsTransformer(queries:Iterator[(Long,it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery)],maxHits:Int,maxHitsPerIndex:Int)* batchSearch(Iterator[(Long,DslQuery)],Int,Int)]],
	  * but returns the ids of the resulting elements.
	  *
	  * @note $ephemeralIDs
		* @group Search
	  */
	def lightBatchSearch(queries: Iterator[(Long, DslQuery)],
	                     maxHits: Int,
	                     maxHitsPerIndex: Int = sameAsMaxHits)
	                    : RDD[(Long, Array[(Long, Double)])]

	/** Like [[batchSearchWithResultsTransformer(queries:Iterator[(Long,it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery)],maxHits:Int,maxHitsPerIndex:Int)* batchSearch(Iterator[(Long,DslQuery)],Int,Int)]],
	  * but uses [[RawQuery]] type queries instead of [[dsl.DslQuery]].
		*
		* @group Search
	  */
	def batchSearchRaw(queries: Iterator[(Long, RawQuery)],
	                   maxHits: Int,
	                   maxHitsPerIndex: Int = sameAsMaxHits)
	                  : RDD[(Long, Array[(T, Double)])]

	/** Like [[batchSearchWithResultsTransformer[V](queries:Iterator[(Long,it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery)],maxHits:Int,transformer:T=>V,maxHitsPerIndex:Int)* batchSearch(Iterator[(Long,DslQuery)],Int,T=>V,Int)]],
	  * but uses [[RawQuery]] type queries instead of [[dsl.DslQuery]].
		*
		* @group Search
	  */
	def batchSearchRawWithResultsTransformer[V](queries: Iterator[(Long, RawQuery)],
	                                            maxHits: Int,
	                                            resultsTransformer: T => V,
	                                            maxHitsPerIndex: Int = sameAsMaxHits)
	                                           : RDD[(Long, Array[(V, Double)])]

	/** Like [[lightBatchSearch(queries:Iterator[(Long,it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery)],maxHits:Int,maxHitsPerIndex:Int)* lightBatchSearch(Iterator[(Long,DslQuery)],Int,Int)]],
	  * but uses [[RawQuery]] type queries instead of [[dsl.DslQuery]].
		*
		* @group Search
	  */
	def lightBatchSearchRaw(queries: Iterator[(Long, RawQuery)],
	                        maxHits: Int,
	                        maxHitsPerIndex: Int = sameAsMaxHits)
	                       : RDD[(Long, Array[(Long, Double)])]

	/** Like [[queryJoinWithResultsTransformer[U](other:org\.apache\.spark\.rdd\.RDD[U],queryGenerator:U=>it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery,maxHits:Int)* queryJoin[U](RDD[U],U=>DslQuery,Int)]],
	  * but with `this` as the `other` RDD; that is, a query self join.
		*
		* @group QueryJoin
	  */
	def queryJoin(queryGenerator: T => DslQuery, maxHits: Int): RDD[(T, Array[(T, Double)])] = queryJoin(this, queryGenerator, maxHits)

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
	def queryJoin[U: ClassTag](other: RDD[U], queryGenerator: U => DslQuery, maxHits: Int): RDD[(U, Array[(T, Double)])]

	/** Like [[queryJoinWithResultsTransformer[U,V](other:org\.apache\.spark\.rdd\.RDD[U],queryGenerator:U=>it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery,maxHits:Int,transformer:T=>V)* queryJoin[U,V](RDD[U],U=>DslQuery,Int,T=>V)]],
	  * but with `this` as the `other` RDD; that is, a query self join.
		*
		* @group QueryJoin
	  */
	def queryJoinWithResultsTransformer[V: ClassTag](queryGenerator: T => DslQuery, maxHits: Int, resultsTransformer: T => V): RDD[(T, Array[(V, Double)])] = queryJoinWithResultsTransformer(this, queryGenerator, maxHits, resultsTransformer)

	/** Like [[queryJoinWithResultsTransformer[U](other:org\.apache\.spark\.rdd\.RDD[U],queryGenerator:U=>it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery,maxHits:Int)* queryJoin[U](RDD[U],U=>DslQuery,Int)]],
	  * but specifying a `transformer` function to be applied to the result elements.
		*
		* @group QueryJoin
	  */
	def queryJoinWithResultsTransformer[U: ClassTag, V: ClassTag](other: RDD[U], queryGenerator: U => DslQuery, maxHits: Int, resultsTransformer: T => V): RDD[(U, Array[(V, Double)])]

	/** Like [[lightQueryJoin[U](other:org\.apache\.spark\.rdd\.RDD[U],queryGenerator:U=>it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery,maxHits:Int)* lightQueryJoin[U](RDD[U],U=>DslQuery,Int)]],
	  * but with `this` as the `other` RDD; that is, a query self join.
		*
		* @group QueryJoin
	  */
	def lightQueryJoin(queryGenerator: T => DslQuery, maxHits: Int): RDD[(T, Array[(Long, Double)])] = lightQueryJoin(this, queryGenerator, maxHits)

	/** Like [[queryJoinWithResultsTransformer[U](other:org\.apache\.spark\.rdd\.RDD[U],queryGenerator:U=>it\.agilelab\.bigdata\.spark\.search\.dsl\.DslQuery,maxHits:Int)* queryJoin[U](RDD[U],U=>DslQuery,Int)]],
	  * but returns the ids of the resulting elements.
	  *
	  * @note $ephemeralIDs
		* @group QueryJoin
	  */
	def lightQueryJoin[U: ClassTag](other: RDD[U], queryGenerator: U => DslQuery, maxHits: Int): RDD[(U, Array[(Long, Double)])]

	/** Returns the element of this `SearchableRDD` with corresponds to the provided `id`, if it exists. */
	def getElement(id: Long): Option[T]

	/** Like [[getElement(id:Long)* getElement(Long)]], but for an `Array` of ids. */
	def getElements(ids: Array[Long]): Array[(Long, Option[T])]
	
	/**
		* Returns the document counts for this SearchableRDD.
		* The key for the map is the field name; the value is the number of documents.
		*/
	def getDocumentCounts: Map[String, Long]
	
	/**
		* Returns the terms counts for this SearchableRDD.
		* The key for the first map is the field name; the one for the second map is the term; the value is the number of
		* occurrences of the term in that field across all documents.
		*/
	def getTermCounts: Map[String, Map[String, Long]]
	
	/**
		* Returns the term IDFs for this SearchableRDD.
		* The key for the first map is the field name; the one for the second map is the term; the value is the IDF of
		* of the term in that field.
		*/
	def getTermIDFs: Map[String, Map[String, Float]] = {
		// obtain document and term counts
		val docCounts = getDocumentCounts
		val termCounts = getTermCounts
		
		calculateTermIDFs(docCounts, termCounts)
	}
	
	/** Returns information about the indices.
		*
		*/
	def getIndicesInfo: IndicesInfo
}
object SearchableRDD {
	/** Constant for default value of `maxHitsPerIndex`. */
	final val sameAsMaxHits = Int.MinValue
	
	/** Returns a new RDD created from `results` by sorting by their score (descending) and truncating
		* it to the top `maxHits` by filtering it.
		*/
	def sortResults[E](results: RDD[(E, Double)], maxHits: Int): RDD[(E, Double)] = {
		results.sortBy({ case (doc, score) => score }, false)
			.zipWithIndex
			.filter { case (res, index) => index < maxHits } // indices start at 0
			.map { case (res, index) => res }
	}
	
	/** Returns an `RDD` built by aggregating the `Iterator`s with the same key in `result` into `Array`s
		* by repetitively choosing the top `maxHits` results in descending score order.
		*/
	def aggregateResultsByKey[E](results: RDD[(Long, Iterator[(E, Double)])], maxHits: Int): RDD[(Long, Array[(E, Double)])] = {
		results.aggregateByKey(Array[(E, Double)]())(
			{
				case (res1, res2) => merge(res1, res2.toArray, maxHits)
			},
			{
				case (res1, res2) => merge(res1, res2, maxHits)
			})
	}
	
	/** Returns the actual value to be used as `maxHitsPerIndex` if it is different from the constant
		* [[sameAsMaxHits]] and it is valid; if it is not valid, throws an exception. If it is equal to
		* [[sameAsMaxHits]], it simply returns `maxHits`.
		*/
	def getMHPIActual(maxHitsPerIndex: Int, maxHits: Int): Int = {
		if (maxHitsPerIndex == sameAsMaxHits)
			maxHits
		else {
			require(maxHitsPerIndex > 0 && maxHitsPerIndex <= maxHits)
			maxHitsPerIndex
		}
	}
	
	/** Combine two maps by summing the values.
		*/
	def combineDocumentCountsMaps(m1: Map[String, Long], m2: Map[String, Long]) = {
		// ++ combines maps replacing items with the same key with the ones form the right-side map
		// the map on termsMap2 makes sure the items on the right of ++ are the sum of any items with the same key
		val docCounts = m1 ++ m2 map {
			case (field, count) => field -> (count + m1.getOrElse(field, 0l))
		}
		
		docCounts
	}
	
	/** Combine two maps-of-maps, assuming the outer maps have the same keyset, by combining each inner map by summing
		* the values.
 		*/
	def combineTermCountsMaps(m1: Map[String, Map[String, Long]], m2: Map[String, Map[String, Long]]) = {
		// map on items from first map
		m1 map {
			case (fieldName, termsMap1) =>
				// get corresponding terms map from second map
				val termsMap2 = m2(fieldName)
				
				// ++ combines maps replacing items with the same key with the ones form the right-side map
				// the map on termsMap2 makes sure the items on the right of ++ are the sum of any items with the same key
				val termsMap = termsMap1 ++ termsMap2 map {
					case (term, count) => term -> (count + termsMap1.getOrElse(term, 0l))
				}
				
				// return new mapping
				fieldName -> termsMap
		}
	}
}