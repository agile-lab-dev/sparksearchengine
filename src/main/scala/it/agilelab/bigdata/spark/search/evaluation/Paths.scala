package it.agilelab.bigdata.spark.search.evaluation


object Paths {
	val rootPath = "/ebs/sparksearch-evaluation/"
	val instanceVolumePaths = List("/disk/ssd0/", "/disk/ssd1/")
	
	val logs = rootPath + "logs/"
	
	val outputDatasets = instanceVolumePaths.head + "datasets/output/"
	
	val enwiki = instanceVolumePaths.last + "enwiki/enwiki-20161120-pages-articles-multistream.xml.bz2"
	val enwikiSubsets = outputDatasets + "enwikiSubsets/"
	
	val clueweb = "/ebs/clueweb12-b13/"
	val cluewebFilesList = "clueweb-warc-files"
	
	val trecWeb2013Topics = "trec/web/2013/trec2013-topics.xml"
	val trecWeb2014Topics = "trec/web/2014/trec2014-topics.xml"
	
	val inex = rootPath + "inex/"
}
