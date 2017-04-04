package it.agilelab.bigdata.spark.search.evaluation


// evaluate sparksearch retrieval performance using inex/clueweb datasets
object RetrievalPerformanceEvaluation extends Evaluation {
	def main(args: Array[String]): Unit = {
		val (name, evaluatorConstructor) = args(0) match {
			case "clueweb" => ("clueweb", new ClueWebRetrievalPerformanceEvaluation(_))
			case "cluewebSingleLuceneIndex" => ("cluewebSingleLuceneIndex", new ClueWebSingleLuceneIndexRetrievalEvaluation(_))
			case "inex" => ???
		}
		
		val log = getLog("retrieval", name)
		runEvaluator(evaluatorConstructor, log)
	}
}


