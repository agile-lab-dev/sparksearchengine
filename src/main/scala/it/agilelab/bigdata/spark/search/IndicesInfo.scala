package it.agilelab.bigdata.spark.search


/** Global information about the indices.
	*/
case class IndicesInfo(numIndices: Int, numDocuments: Long, sizeBytes: Long, indicesInfo: List[IndexInfo]) {
	def prettyToString(): String = {
		val localIndicesInfos = indicesInfo.zipWithIndex map {
			case (indexInfo, indexNumber) =>
				s"""|   index $indexNumber info:
						|     num segments : ${indexInfo.numSegments}
				    |     num documents: ${indexInfo.numDocuments}
			      |     size bytes   : ${indexInfo.sizeBytes}""".stripMargin
		}
		val globalIndicesInfo =
			s"""Indices info:
					| num indices  : $numIndices
					| num documents: $numDocuments
		      | size bytes   : $sizeBytes""".stripMargin
		
		globalIndicesInfo + "\n" + localIndicesInfos.mkString("\n")
	}
}

/** Information about an index.
	*/
case class IndexInfo(numDocuments: Int, sizeBytes: Long, numSegments: Int) {
	def prettyToString(): String = {
			s"""|Index info:
			    |   num segments : $numSegments
			    |   num documents: $numDocuments
			    |   size bytes   : $sizeBytes""".stripMargin
	}
}
