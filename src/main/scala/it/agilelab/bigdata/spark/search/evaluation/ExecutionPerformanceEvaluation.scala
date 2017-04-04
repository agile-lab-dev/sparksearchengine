package it.agilelab.bigdata.spark.search.evaluation

// evaluate sparksearch/elasticsearch execution performance using wikipedia dump
// test indexing time, size for various subsets and query time for various queries (and/or, many/few terms, common/rare terms)
object ExecutionPerformanceEvaluation extends Evaluation {
	def main(args: Array[String]): Unit = {
		val (name, evaluatorConstructor) = args(0) match {
			case "sparksearch" => ("sparksearch", new SparksearchExecutionPerformanceEvaluator(_))
			case "elasticsearch" => ("elasticsearch", new ElasticsearchExecutionPerformanceEvaluator(_))
		}
		
		val log = getLog("performance", name)
		runEvaluator(evaluatorConstructor, log)
	}
}


