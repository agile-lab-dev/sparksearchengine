package it.agilelab.bigdata.spark.search.utils

import org.slf4j.{Logger, LoggerFactory}

/**
	* Logging helper trait.
	*/
trait Logging {
	// transient so objects with this trait are still serializable
	@transient private var logger_ : Logger = null

	// get or create the logger for this object
	protected def logger: Logger = {
		if (logger_ == null) {
			logger_ = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$")) // ignore trailing $ for Scala objects
		}
		logger_
	}
}
