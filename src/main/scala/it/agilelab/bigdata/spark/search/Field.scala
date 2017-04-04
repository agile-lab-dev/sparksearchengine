package it.agilelab.bigdata.spark.search


import java.util.Date

import scala.math._

sealed abstract class Field[T](val name: String, val value: T) extends Serializable {
	override def toString = {
		val string = value.toString
		"(" + name + "," + string.substring(0, min(string.length, 32)) + ")"
	}
}

case class StringField(_name: String, _value: String) extends Field[String](_name, _value)

case class NoPositionsStringField(_name: String, _value: String) extends Field[String](_name, _value)

case class IntField(_name: String, _value: Int) extends Field[Int](_name, _value)

case class LongField(_name: String, _value: Long) extends Field[Long](_name, _value)

case class FloatField(_name: String, _value: Float) extends Field[Float](_name, _value)

case class DoubleField(_name: String, _value: Double) extends Field[Double](_name, _value)

case class DateField(_name: String, _value: Date) extends Field[Date](_name, _value)

case class SeqField(_name: String, _value: Seq[_]) extends Field[Seq[_]](_name, _value)