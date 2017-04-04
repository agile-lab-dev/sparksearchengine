package it.agilelab.bigdata.spark.search.impl.similarities

import java.util

import org.apache.lucene.search.similarities.ClassicSimilarity
import org.apache.lucene.search.{CollectionStatistics, Explanation, TermStatistics}

/** Variant of ClassicSimilarity supporting global IDF using a map for IDF values of terms.
	* The key for the first map is the field name; the one for the second map is the term; the value is the IDF for the
	* term in that field.
	*
	* @author Nicolò Bidotti
	*/
class TFIDFWithGlobalIDFSimilarity(globalIdf: Map[String, Map[String, Float]]) extends ClassicSimilarity {
	/**
		* Computes a score factor for a simple term and returns an explanation
		* for that score factor.
		*
		* <p>
		* The default implementation uses:
		*
		* <pre class="prettyprint">
		* idf(docFreq, docCount);
		* </pre>
		*
		* Note that {@link CollectionStatistics#docCount()} is used instead of
		* {@link org.apache.lucene.index.IndexReader#numDocs() IndexReader#numDocs()} because also
		* {@link TermStatistics#docFreq()} is used, and when the latter
		* is inaccurate, so is {@link CollectionStatistics#docCount()}, and in the same direction.
		* In addition, {@link CollectionStatistics#docCount()} does not skew when fields are sparse.
		*
		* @param collectionStats collection-level statistics
		* @param termStats       term-level statistics for the term
		* @return an Explain object that includes both an idf score factor
		*         and an explanation for the term.
		*/
	override def idfExplain(collectionStats: CollectionStatistics, termStats: TermStatistics): Explanation = {
		val field = collectionStats.field()
		val term = termStats.term()
		// TODO: using utf8ToString on BytesRef can fail if the term represented by BytesRef is not a string!!!
		val termString = term.utf8ToString()
		val idf = globalIdf(field)(termString)
		Explanation.`match`(idf, "globalIdf(term=" + termString + ")")
	}
	
	/**
		* Computes a score factor for a phrase.
		*
		* <p>
		* The default implementation sums the idf factor for
		* each term in the phrase.
		*
		* @param collectionStats collection-level statistics
		* @param termStats       term-level statistics for the terms in the phrase
		* @return an Explain object that includes both an idf
		*         score factor for the phrase and an explanation
		*         for each term.
		*/
	override def idfExplain(collectionStats: CollectionStatistics, termStats: Array[TermStatistics]): Explanation = {
		val field = collectionStats.field()
		var idf = 0.0f
		val subs = new util.ArrayList[Explanation]
		for (stat <- termStats) {
			val term = stat.term()
			// TODO: using utf8ToString on BytesRef can fail if the term represented by BytesRef is not a string!!!
			val termString = term.utf8ToString()
			val termIdf = globalIdf(field)(termString)
			subs.add(Explanation.`match`(termIdf, "globalIdf(term=" + termString + ")"))
			idf += termIdf
		}
		Explanation.`match`(idf, "idf(), sum of:", subs)
	}
	
	// override idf from base class so as to ensure we never use it
	override def idf(docFreq: Long, docCount: Long): Float = throw new NotImplementedError("This should never be used! Use globalIdf instead!")
}
