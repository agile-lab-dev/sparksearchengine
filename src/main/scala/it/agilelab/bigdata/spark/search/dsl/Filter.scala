package it.agilelab.bigdata.spark.search.dsl

import scala.collection.JavaConverters._

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.document.{DoublePoint, FloatPoint, IntPoint, LongPoint}
import org.apache.lucene.index.Term
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.{Query => LuceneQuery, _}

abstract class Filter extends DslQuery {
	protected def rangeQuery[T](
		  field: field,
		  minValue: value[T],
		  maxValue: value[T],
		  minInclusive: Boolean,
		  maxInclusive: Boolean): LuceneQuery = {
		Option(minValue).getOrElse(maxValue) match { // match type of a value that's not null
			case _: stringValue => {
				val min = Option(minValue.asInstanceOf[stringValue]).getOrElse(stringValue(null)).value
				val max = Option(maxValue.asInstanceOf[stringValue]).getOrElse(stringValue(null)).value
				TermRangeQuery.newStringRange(field.name, min, max, minInclusive, maxInclusive)
			}
			case _: intValue => {
				val min = Option(minValue.asInstanceOf[intValue]).getOrElse(intValue(Int.MinValue)).value
				val max = Option(maxValue.asInstanceOf[intValue]).getOrElse(intValue(Int.MaxValue)).value
				val lowerValue = if (minInclusive) min else Math.addExact(min, 1)
				val upperValue = if (maxInclusive) max else Math.addExact(max, -1)
				IntPoint.newRangeQuery(field.name, lowerValue, upperValue)
			}
			case _: longValue => {
				val min = Option(minValue.asInstanceOf[longValue]).getOrElse(longValue(Long.MinValue)).value
				val max = Option(maxValue.asInstanceOf[longValue]).getOrElse(longValue(Long.MaxValue)).value
				val lowerValue = if (minInclusive) min else Math.addExact(min, 1)
				val upperValue = if (maxInclusive) max else Math.addExact(max, -1)
				LongPoint.newRangeQuery(field.name, lowerValue, upperValue)
			}
			case _: floatValue => {
				val min = Option(minValue.asInstanceOf[floatValue]).getOrElse(floatValue(Float.MinValue)).value
				val max = Option(maxValue.asInstanceOf[floatValue]).getOrElse(floatValue(Float.MaxValue)).value
				val lowerValue = if (minInclusive) min else Math.nextUp(min)
				val upperValue = if (maxInclusive) max else Math.nextDown(max)
				FloatPoint.newRangeQuery(field.name, lowerValue, upperValue)
			}
			case _: doubleValue => {
				val min = Option(minValue.asInstanceOf[doubleValue]).getOrElse(doubleValue(Double.MinValue)).value
				val max = Option(maxValue.asInstanceOf[doubleValue]).getOrElse(doubleValue(Double.MaxValue)).value
				val lowerValue = if (minInclusive) min else Math.nextUp(min)
				val upperValue = if (maxInclusive) max else Math.nextDown(max)
				DoublePoint.newRangeQuery(field.name, lowerValue, upperValue)
			}
		}
	}

	protected def exactQuery[T](
		  field: field,
		  value: value[T]): LuceneQuery = {
		value match { // match type of value
			case v: stringValue => new TermQuery(new Term(field.name, v.value))
			case v: intValue => IntPoint.newExactQuery(field.name, v.value)
			case v: longValue => LongPoint.newExactQuery(field.name, v.value)
			case v: floatValue => FloatPoint.newExactQuery(field.name, v.value)
			case v: doubleValue => DoublePoint.newExactQuery(field.name, v.value)
		}
	}

	protected def setQuery[T](
		  field: field,
		  valueSet: valueSet[T]): LuceneQuery = {
		valueSet.values.head match { // match type of values
			case _: stringValue => {
				val builder = new BooleanQuery.Builder()
				for (value <- valueSet.values) {
					builder.add(exactQuery(field, value), BooleanClause.Occur.SHOULD)
				}
				builder.build()
			}
			case _: intValue => {
				val javaIntegerValues = valueSet.values map {
					value => value.asInstanceOf[intValue].value.asInstanceOf[java.lang.Integer]
				}
				val javaList = seqAsJavaListConverter(javaIntegerValues).asJava
				IntPoint.newSetQuery(field.name, javaList)
			}
			case _: longValue => {
				val javaLongValues = valueSet.values map {
					value => value.asInstanceOf[longValue].value.asInstanceOf[java.lang.Long]
				}
				val javaList = seqAsJavaListConverter(javaLongValues).asJava
				LongPoint.newSetQuery(field.name, javaList)
			}
			case _: floatValue => {
				val javaFloatValues = valueSet.values map {
					value => value.asInstanceOf[floatValue].value.asInstanceOf[java.lang.Float]
				}
				val javaList = seqAsJavaListConverter(javaFloatValues).asJava
				FloatPoint.newSetQuery(field.name, javaList)
			}
			case _: doubleValue => {
				val javaDoubleValues = valueSet.values map {
					value => value.asInstanceOf[doubleValue].value.asInstanceOf[java.lang.Double]
				}
				val javaList = seqAsJavaListConverter(javaDoubleValues).asJava
				DoublePoint.newSetQuery(field.name, javaList)
			}
		}
	}
}

case class EqualToFilter[T](field: field, value: value[T]) extends Filter {
	override def getLuceneQuery(analyzer: Analyzer): LuceneQuery = {
		val builder = new BooleanQuery.Builder()
		val filterQuery = exactQuery(field, value)
		builder.add(filterQuery, BooleanClause.Occur.FILTER).build()
	}
}

case class GreaterThanFilter[T](field: field, value: value[T]) extends Filter {
	override def getLuceneQuery(analyzer: Analyzer): LuceneQuery = {
		val builder = new BooleanQuery.Builder()
		val filterQuery = rangeQuery(field, value, null, false, true)
		builder.add(filterQuery, BooleanClause.Occur.FILTER).build()
	}
}

case class GreaterThanOrEqualToFilter[T](field: field, value: value[T]) extends Filter {
	override def getLuceneQuery(analyzer: Analyzer): LuceneQuery = {
		val builder = new BooleanQuery.Builder()
		val filterQuery = rangeQuery(field, value, null, true, true)
		builder.add(filterQuery, BooleanClause.Occur.FILTER).build()
	}
}

case class LowerThanFilter[T](field: field, value: value[T]) extends Filter {
	override def getLuceneQuery(analyzer: Analyzer): LuceneQuery = {
		val builder = new BooleanQuery.Builder()
		val filterQuery = rangeQuery(field, null, value, true, false)
		builder.add(filterQuery, BooleanClause.Occur.FILTER).build()
	}
}

case class LowerThanOrEqualToFilter[T](field: field, value: value[T]) extends Filter {
	override def getLuceneQuery(analyzer: Analyzer): LuceneQuery = {
		val builder = new BooleanQuery.Builder()
		val filterQuery = rangeQuery(field, null, value, true, true)
		builder.add(filterQuery, BooleanClause.Occur.FILTER).build()
	}
}

case class RangeFilter[T](field: field, range: range[T]) extends Filter {
	require(range.min != null && range.max != null, "Range boundaries must not be null.")
	override def getLuceneQuery(analyzer: Analyzer): LuceneQuery = {
		val builder = new BooleanQuery.Builder()
		val filterQuery = rangeQuery(field, range.min, range.max, true, true)
		builder.add(filterQuery, BooleanClause.Occur.FILTER).build()
	}
}

case class ValueSetFilter[T](field: field, valueSet: valueSet[T]) extends Filter {
	override def getLuceneQuery(analyzer: Analyzer): LuceneQuery = {
		val builder = new BooleanQuery.Builder()
		val filterQuery = setQuery(field, valueSet)
		builder.add(filterQuery, BooleanClause.Occur.FILTER).build()
	}
}

case class FieldExistsFilter(field: field) extends Filter {
	override def getLuceneQuery(analyzer: Analyzer): LuceneQuery = {
		val builder = new BooleanQuery.Builder()
		val filterQuery = TermRangeQuery.newStringRange(field.name, null, null, true, true)
		builder.add(filterQuery, BooleanClause.Occur.FILTER).build()
	}
}

case class FieldMissingFilter(field: field) extends Filter {
	override def getLuceneQuery(analyzer: Analyzer): LuceneQuery = {
		val builder = new BooleanQuery.Builder()
		val filterQuery = builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST)
			.add(TermRangeQuery.newStringRange(field.name, null, null, false, false), BooleanClause.Occur.MUST_NOT)
		  .build()
		val builder2 = new BooleanQuery.Builder()
		builder2.add(filterQuery, BooleanClause.Occur.FILTER).build()
	}
}