package it.agilelab.bigdata.spark.search.evaluation.utils

import it.agilelab.bigdata.spark.search._


case class cluewebpage(trecId: String, content: String) extends Indexable with Storeable[String] {
	override def getFields: Iterable[Field[_]] = Seq(
		StringField("trecId", trecId),
		NoPositionsStringField("content", content)
	)
	
	override def getData: String = trecId
}
