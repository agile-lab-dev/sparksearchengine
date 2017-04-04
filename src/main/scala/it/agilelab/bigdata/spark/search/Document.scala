package it.agilelab.bigdata.spark.search

class Document(val id: Long, val content: Iterable[Field[String]]) extends Serializable with Indexable {
	def getFieldValues(name: String) = {
		content.filter(field => field.name.equals(name)).map(field => field.value)
	}

	override def toString = "id: " + id + "; content: " + content.mkString(",")

	override def getFields: Iterable[Field[String]] = content
}


