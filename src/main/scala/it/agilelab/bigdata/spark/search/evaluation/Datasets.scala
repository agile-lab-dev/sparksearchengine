package it.agilelab.bigdata.spark.search.evaluation

import it.agilelab.bigdata.spark.search.evaluation.utils.{cluewebpage, wikipage}

trait Dataset[T] {
	def name: String
	def desc: String
	def path: String
	
	override def toString: String = "Dataset info:\n" +
		"\tname: " + name + "\n" +
	  "\tdesc: " + desc + "\n" +
	  "\tpath: " + path + "\n"
}

object Datasets {
	val all = List(enwiki, ClueWeb12_B13, INEX)
	
	object enwiki extends Dataset[wikipage] {
		override def name: String = "enwiki-20161101"
		override def desc: String = "English Wikipedia dump from 01/11/2016"
		override def path: String = Paths.enwiki
	}
	object ClueWeb12_B13 extends Dataset[cluewebpage] {
		override def name: String = "ClueWeb12-B13"
		override def desc: String = "ClueWeb12 dataset (B13 subset)"
		override def path: String = Paths.clueweb
	}
	object INEX extends Dataset[Any] {
		override def name: String = "INEX"
		override def desc: String = "INEX Wikipedia 2009 collection"
		override def path: String = Paths.inex
	}
}
