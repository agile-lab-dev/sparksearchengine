package it.agilelab.bigdata.spark.search.evaluation

import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.util.Date


abstract class Evaluator(logFile: PrintWriter) extends SparkApplication {
	def name: String
	def desc: String
	
	// invoked once
	def prepare(): Unit
	// invoked numRuns times
	def run(): Unit
	
	// log to log file & stdout
	val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
	def log(message: String): Unit = {
		val datedMessage = formatter.format(new Date()) + " " + message
		logFile.println(datedMessage)
		logFile.flush()
		println(datedMessage)
	}
	
	// helpers for timing stuff
	def now: Long = System.nanoTime()
	def secs(start: Long, end: Long): Double = (end-start)/Math.pow(10, 9)
}
