package it.agilelab.bigdata.spark.search.utils

/** Utility methods for IDF calculations
	*
	* @author Nicolò Bidotti
	*/
object IDFUtils {
	// utility method to calculate term IDFs from document and term counts
	def calculateTermIDFs(docCounts: Map[String, Long], termCounts: Map[String, Map[String, Long]]): Map[String, Map[String, Float]] = {
		// create IDF map by joining by key and applying IDF function
		val termIDFs = docCounts map {
			case (field, docCount) => {
				val fieldIDFs = termCounts(field) map {
					case (term, termFreq) => {
						val termIdf = idf(termFreq, docCount)
						term -> termIdf
					}
				}
				field -> fieldIDFs
			}
		}
		
		termIDFs
	}
	
	// utility method to calculate idf from term frequency and document count
	// idf is calculated as log(1 + (docCount - docFreq + 0.5)/(docFreq + 0.5))
	def idf(termFreq: Long, docCount: Long): Float = Math.log(1 + (docCount - termFreq + 0.5D) / (termFreq + 0.5D)).toFloat
	
}
