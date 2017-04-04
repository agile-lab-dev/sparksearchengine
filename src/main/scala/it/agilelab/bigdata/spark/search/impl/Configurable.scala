package it.agilelab.bigdata.spark.search.impl

/**
 * A configurable component, such that its configuration can be saved when it is
 * serialized and restored when deserialized.
 * This strategy is adopted because the majority of the Lucene classes are not serializable,
 * so a workaround is used: we only serialize the information needed to rebuild.
 * This includes the class names for the Analyzer/QueryConstructor, which must have a
 * single arg constructor, taking the config as the parameter, and a getConfig() method.
 * The constructor will be invoked on deserialization, and must rebuild it from scratch.
 * The getConfig() will be invoked on serialization, and must save in the returned config
 * all the information necessary to rebuild it.
 * This solution will hopefully be improved upon in a future release. 
 */
abstract class Configurable(val config: LuceneConfig) {
	def getConfig: LuceneConfig
}