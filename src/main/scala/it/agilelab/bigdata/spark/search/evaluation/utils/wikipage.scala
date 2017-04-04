package it.agilelab.bigdata.spark.search.evaluation.utils

import it.agilelab.bigdata.spark.search.{Field, Indexable, Storeable, StringField}


case class wikipage(title: String, text: String) extends Indexable with Storeable[String] {
	override def getFields: Iterable[Field[_]] = Seq(
		StringField("text", text),
		StringField("title", title)
	)
	
	override def getData: String = text
}
