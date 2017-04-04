package it.agilelab.bigdata.spark.search.dsl


import scala.language.implicitConversions

/**
	* Query DSL grammar:
	*
	* QUERY =
	*   bestResult of QUERY
	*   topResults NUMBER of QUERY
	*   scoreCutoff NUMBER of QUERY
	*
	* QUERY =
	*   all
	*   not QUERY
	*   QUERY && QUERY
	*   QUERY || QUERY
	*
	* QUERY =
	*   FIELDNAME matchText "text"
	*   FIELDNAME matchPhrase "phrase"
	*   FIELDNAME matchTerm TERM
	*   FIELDNAME matchMin NUMBER from TERMSET
	*   FIELDNAME matchAll TERMSET
	*   FIELDNAME matchAny TERMSET
	*   FIELDNAME parseQuery "raw lucene query"
	*   FILTER
	*
	* FILTER =
	*   FIELDNAME e FIELDVALUE
	*   FIELDNAME gt Num
	*   FIELDNAME gte Num
	*   FIELDNAME lt Num
	*   FIELDNAME lte Num
	*   FIELDNAME in RANGE
	*   FIELDNAME in VALUESET
	*   exists(FIELDNAME)
	*   missing(FIELDNAME)
	*
	* TERM = term("term")
	* FIELDNAME = field("field")
	* FIELDVALUE = value("value")
	*
	* TERMSET = termSet(TERM...)
	* VALUESET = valueSet(FIELDVALUE..)
	* RANGE = range(FIELDVALUE, FIELDVALUE)
	*
	*/

/**
	* The main entry point for the Query DSL.
	*/
case class QueryBuilder(field: field) {
	// queries ==================================================================

	def matchText(text: String): MatchTextQuery = MatchTextQuery(field, text)
	
	def matchAllText(text: String): MatchAllTextQuery = MatchAllTextQuery(field, text)

	def matchPhrase(phrase: String): MatchPhraseQuery = MatchPhraseQuery(field, phrase)

	def matchTerm(term: term): MatchTermQuery = MatchTermQuery(field, term)

	def matchMin(num: Int): MatchMinQueryExpectsTermSet = MatchMinQueryExpectsTermSet(field, num)

	def matchAll(termSet: termSet): MatchAllQuery = MatchAllQuery(field, termSet)

	def matchAny(termSet: termSet): MatchAnyQuery = MatchAnyQuery(field, termSet)

	def parseQuery(query: String): ParsedQuery = ParsedQuery(field, query)

	// filters ==================================================================

	def eq[T](value: value[T]): EqualToFilter[T] = EqualToFilter(field, value)

	def gt[T](value: value[T]): GreaterThanFilter[T] = GreaterThanFilter(field, value)

	def ge[T](value: value[T]): GreaterThanOrEqualToFilter[T] = GreaterThanOrEqualToFilter(field, value)

	def lt[T](value: value[T]): LowerThanFilter[T] = LowerThanFilter(field, value)

	def le[T](value: value[T]): LowerThanOrEqualToFilter[T] = LowerThanOrEqualToFilter(field, value)

	def in[T](range: range[T]): RangeFilter[T] = RangeFilter(field, range)

	def in[T](valueSet: valueSet[T]): ValueSetFilter[T] = ValueSetFilter(field, valueSet)
}

/*
this allows to use the DSL with a single package import
*/
object `package` {
	implicit def fieldString2QueryBuilder(field: String): QueryBuilder = QueryBuilder(field)

	implicit def string2term(s: String): term = term(s)

	implicit def string2stringValue(s: String): stringValue = stringValue(s)

	implicit def int2intValue(i: Int): intValue = intValue(i)

	implicit def long2longValue(l: Long): longValue = longValue(l)

	implicit def float2floatValue(f: Float): floatValue = floatValue(f)

	implicit def double2doubleValue(d: Double): doubleValue = doubleValue(d)

	def all: AllDocsQuery = new AllDocsQuery()

	def not(query: DslQuery): NegatedQuery = new NegatedQuery(query)

	def exists(field: field): FieldExistsFilter = FieldExistsFilter(field)

	def missing(field: field): FieldMissingFilter = FieldMissingFilter(field)
}

/**
	* An indexed field.
	*
	* @param name
	*/
case class field(name: String)
object field {
	implicit def string2field(name: String): field = field(name)
}

/**
	* A term.
	*
	* @param term
	*/
case class term(term: String)

/**
	* A set of terms.
	*
	* @param termStrings
	*/
case class termSet(termStrings: String*) {
	val terms = termStrings.map(t => term(t))
}

/**
	* A field value.
	*
	* @param value
	* @tparam T
	*/
/*
value needs to explicitly implement Serializable because
it is abstract and does not have an empty constructor;
this way, the subclasses must take care of everything, but since
they are case classes, we don't actually need to do anything
*/
sealed abstract class value[T](value: T) extends Serializable
case class stringValue(value: String) extends value[String](value)
case class intValue(value: Int) extends value[Int](value)
case class longValue(value: Long) extends value[Long](value)
case class floatValue(value: Float) extends value[Float](value)
case class doubleValue(value: Double) extends value[Double](value)

/**
	* A set of values.
	*
	* @param values
	* @tparam T
	*/
case class valueSet[T](values: value[T]*)

/**
	* A range of values.
	*
	* @param min
	* @param max
	* @tparam T
	*/
case class range[T](min: value[T], max: value[T])