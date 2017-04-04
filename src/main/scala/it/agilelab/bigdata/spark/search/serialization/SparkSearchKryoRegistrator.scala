package it.agilelab.bigdata.spark.search.serialization

import com.esotericsoftware.kryo.Kryo
import it.agilelab.bigdata.spark.search.impl._
import org.apache.spark.serializer.KryoRegistrator


class SparkSearchKryoRegistrator extends KryoRegistrator {
	override def registerClasses(kryo: Kryo): Unit = {
		kryo.register(classOf[PartitionsIndexLuceneRDD[_]])
		kryo.register(classOf[LuceneIndexedPartition[_]])
		kryo.register(classOf[LuceneIndex])
		kryo.register(classOf[GlobalIDFLuceneIndex])
		kryo.register(classOf[LuceneConfig])
	}
}
