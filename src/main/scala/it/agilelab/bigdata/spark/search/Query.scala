package it.agilelab.bigdata.spark.search


abstract class Query extends Serializable

case class RawQuery(queryString: String) extends Query
