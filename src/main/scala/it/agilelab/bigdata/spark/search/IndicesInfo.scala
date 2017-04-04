package it.agilelab.bigdata.spark.search


/** Global information about the indices.
	*/
case class IndicesInfo(numIndices: Int, numDocuments: Long, sizeBytes: Long, indicesInfo: List[IndexInfo])

/** Information about an index.
	*/
case class IndexInfo(numDocuments: Int, sizeBytes: Long, numSegments: Int)
