package it.agilelab.bigdata.spark.search.examples

import it.agilelab.bigdata.spark.search.evaluation.utils.wikipage
import it.agilelab.bigdata.spark.search.impl.{LuceneConfig, PartitionsIndexLuceneRDD}
import it.agilelab.bigdata.spark.search.{Field, Storeable, StringField}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
	* Example to show use of SearchableRDD with data implementing the Storeable trait.
	*/
object StoreableExample {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Parse Wikipedia dump file")
		val sc = SparkContext.getOrCreate(conf)
		
		val inputPath = args(0)
		val outputPath = args(1)
		
		val wikipages = sc.objectFile[wikipage](inputPath).coalesce(30, false)
		
		val storeable = wikipages map (wp => storeableWikipageTitle(wp))
		
		val searchable = PartitionsIndexLuceneRDD(storeable.asInstanceOf[RDD[Storeable[String]]], LuceneConfig.defaultConfig(), "dummy")
		
		searchable.persist(StorageLevel.MEMORY_AND_DISK)
		
		searchable.saveAsObjectFile(outputPath)
		
		println(searchable.getDataAndIndicesInfo)
	}
}

case class storeableWikipageTitle(wp: wikipage) extends Storeable[String] {
	override def getFields: Iterable[Field[_]] = {
		Array(
			StringField("text", wp.text),
			StringField("title", wp.title)
		)
	}
	override def getFields(prefix: String): Iterable[Field[_]] = super.getFields(prefix)
	override def getData: String = wp.title
}