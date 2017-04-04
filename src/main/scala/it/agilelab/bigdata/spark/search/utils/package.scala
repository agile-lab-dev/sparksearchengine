package it.agilelab.bigdata.spark.search

import scala.math.min

package object utils {
	/**
		* Merges two arrays of tuples of type `(T, Double)`, sorted in descending order according to the second element,
		* into one array of up to `max` size, sorted in descending order.
		*
		* @param a1 the first array
		* @param a2 the second array
		* @param max the maximum size of the returned array
		* @tparam T the type of the first element of the tuple
		* @return a new array of up to `max` size, containing the tuples from the input arrays, in descending order
		*/
	def merge[T](a1: Array[(T, Double)], a2: Array[(T, Double)], max: Int): Array[(T, Double)] = {
		val res = new Array[(T, Double)](min(a1.length + a2.length, max))
		var i, j, k = 0
		while(k < max && i < a1.length && j < a2.length) {
			if (a1(i)._2 >= a2(j)._2) {
				res(k) = a1(i)
				i += 1
			} else {
				res(k) = a2(j)
				j += 1
			}
			k += 1
		}
		while(k < max && i < a1.length) {
			res(k) = a1(i)
			i += 1
			k += 1
		}
		while(k < max && j < a2.length) {
			res(k) = a2(j)
			j += 1
			k += 1
		}
		res
	}

	/**
		* Merges two iterators of tuples of type `(T, Double)`, sorted in descending order according to the second element,
		* into one iterator of up to `max` size, sorted in descending order.
		*
		* @note Only works if iterators never return null values! This however should not be a problem, as null values cannot
		*       be ordered anyway.
		* @param i1 the first iterator
		* @param i2 the second iterator
		* @param maxSize the maximum size of the returned iterator
		* @tparam T the type of the first element of the tuple
		* @return a new iterator of up to `max` size, containing the tuples from the input iterators, in descending order
		*/
	def mergeIterators[T](
		  i1: Iterator[(T, Double)],
		  i2: Iterator[(T, Double)],
		  maxSize: Int): Iterator[(T, Double)] = {
		new Iterator[(T, Double)] {
			// to track size of iterator
			var i = 0
			// to track first element of each iterator
			var e1 = nextOrNull(i1)
			var e2 = nextOrNull(i2)

			def hasNext: Boolean = (i < maxSize) && (i1.hasNext || e1 != null || i2.hasNext || e2 != null)

			def next: (T, Double) = {
				// choose value to return, update used elements
				// sorry for nested ifs, they're this way to make the absolute minimum number of checks
				var nextElement: (T, Double) = null
				if (e1 != null) {
					if (e2 != null) {
						// both non-null: return greatest
						if (e1._2 >= e2._2) {
							nextElement = e1
							e1 = nextOrNull(i1)
						} else {
							nextElement = e2
							e2 = nextOrNull(i2)
						}
					} else {
						// e1 non-null, e2 null: return e1
						nextElement = e1
						e1 = nextOrNull(i1)
					}
				} else {
					if (e2 != null) {
						// e1 null, e2 non-null: return e2
						nextElement = e2
						e2 = nextOrNull(i2)
					} else {
						// both null: exception, next called on empty iterator!
						throw new UnsupportedOperationException("Called next on empty Iterator!")
					}
				}
				nextElement
			}

			private def nextOrNull(i: Iterator[(T, Double)]) = if (i.hasNext) i.next() else null
		}
	}
}
