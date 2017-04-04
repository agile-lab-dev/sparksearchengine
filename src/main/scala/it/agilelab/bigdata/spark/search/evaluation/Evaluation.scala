package it.agilelab.bigdata.spark.search.evaluation

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

/** Generic evaluation execution code.
	*
	*/
trait Evaluation {
	def getLog(evaluationType: String, name: String): PrintWriter = {
		val timeString = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date())
		val logPath = Paths.logs + evaluationType + File.separator + name + File.separator
		new File(logPath).mkdirs()
		val log = new PrintWriter(logPath + timeString + ".log")
		
		log
	}
	
	def runEvaluator(evaluatorConstructor: (PrintWriter) => Evaluator, log: PrintWriter): Unit = {
		val evaluator = evaluatorConstructor(log)
		evaluator.prepare()
		evaluator.run()
	}
}
