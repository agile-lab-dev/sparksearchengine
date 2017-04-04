package it.agilelab.bigdata.spark.search.evaluation

import java.io.{DataInputStream, FileInputStream, PrintWriter}
import java.util.concurrent._

import edu.cmu.lemurproject.{WarcHTMLResponseRecord, WarcRecord}
import it.agilelab.bigdata.spark.search.dsl._
import it.agilelab.bigdata.spark.search.evaluation.utils.{IngestClueWeb, cluewebpage}
import it.agilelab.bigdata.spark.search.impl.{BigChunksRAMDirectory, CustomCodec, LuceneIndexedPartition}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.util.ReflectionUtils
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.document.StoredField
import org.apache.lucene.index._
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.similarities.BM25Similarity

class ClueWebSingleLuceneIndexRetrievalEvaluation(pw: PrintWriter) extends ClueWebRetrievalPerformanceEvaluation(pw) {
	import IngestClueWeb._
	import LuceneIndexedPartition._
	
	override def run(): Unit = {
		// prepare index
		log("Preparing index")
		val directory = new BigChunksRAMDirectory()
		val indexWriterConfig = buildIndexWriterConfig
		val indexWriter = new IndexWriter(directory, indexWriterConfig)
		log("Done")
		
		// prepare clueweb
		log("Preparing documents")
		val warcFiles = IngestClueWeb.listWarcFilesUsingList(Paths.clueweb).split(",").iterator
		val hadoopConf = sc.hadoopConfiguration
		val codecClass = this.getClass.getClassLoader.loadClass("org.apache.hadoop.io.compress.GzipCodec").asSubclass(classOf[CompressionCodec])
		val warcDIS = warcFiles map { path =>
			log("Processing \"" + path + "\"")
			val fis = new FileInputStream(path)
			val compressionCodec = ReflectionUtils.newInstance(codecClass, hadoopConf)
			val decompressedData = compressionCodec.createInputStream(fis)
			new DataInputStream(decompressedData)
		}
		val warcRecordsIterators = warcDIS map { dis => getWarcRecordsIterator(dis) }
		val htmlReponsesIterators = warcRecordsIterators map { warcRecords =>
			warcRecords.filter(_.getHeaderRecordType == "response")
				.map(record => new WarcHTMLResponseRecord(record))
		}
		val cluewebpagesIterators = htmlReponsesIterators map { htmlResponses =>
			htmlResponses.map {
				response =>
					val trecID = response.getTargetTrecID
					val rawContent = response.getRawRecord
						.getContentUTF8 // get content as utf8 string
					val content = getTextFromHTMLJsoup(rawContent)
					
					cluewebpage(trecID, content)
			}
		}
		log("Done")
		
		// parallel indexing
		log("Indexing")
		val numWorkers = sys.runtime.availableProcessors
		val queueCapacity = numWorkers * 2
		log(s"Using $numWorkers threads, queue size $queueCapacity")
		val pool = new ThreadPoolExecutor( // threadpoolexecutor with blocking submit
			numWorkers, numWorkers,
			0L, TimeUnit.SECONDS,
			new ArrayBlockingQueue[Runnable](queueCapacity) {
				override def offer(e: Runnable) = {
					put(e); // may block if waiting for empty room
					true
				}
			}
		)
		val runnables = cluewebpagesIterators map { cluewebpages =>
			new Runnable {
				override def run(): Unit = cluewebpages foreach { cluewebpage =>
					val document = indexable2LuceneDocument(cluewebpage)
					document.add(new StoredField("trecId", cluewebpage.trecId))
					indexWriter.addDocument(document)
				}
			}
		}
		var numFiles = 0
		runnables foreach { runnable =>
			pool.submit(runnable)
			log(s"Submitted #$numFiles")
			numFiles += 1
		}
		
		// wait until all runnables are done
		pool.shutdown()
		pool.awaitTermination(Long.MaxValue, TimeUnit.DAYS)
		
		// close index
		indexWriter.getConfig.setUseCompoundFile(true)
		indexWriter.close()
		log("Done")
		
		// open index for searching
		log("Opening index for searching")
		val indexReader = DirectoryReader.open(directory)
		val indexSearcher = new IndexSearcher(indexReader)
		log("Done")
		
		log(s"Index info: ${indexReader.numDocs()} documents, ${indexReader.getContext.children().size()} segments, ${directory.ramBytesUsed()} bytes")
		
		// run queries
		log("Loading queries")
		val queries2013 = loadQueries(this.getClass.getClassLoader.getResourceAsStream(Paths.trecWeb2013Topics))
		val queries2014 = loadQueries(this.getClass.getClassLoader.getResourceAsStream(Paths.trecWeb2014Topics))
		log("Done")
		log("Running queries")
		val results2013 = queries2013 map { case (queryId, query) => (queryId, search(indexReader, indexSearcher, query, 1000)) }
		log("Done for 2013")
		val results2014 = queries2014 map { case (queryId, query) => (queryId, search(indexReader, indexSearcher, query, 1000)) }
		log("Done for 2014")
		
		log("Converting to TREC format")
		val results2013TrecFormat = formatResultsToTrec(results2013)
		val results2014TrecFormat = formatResultsToTrec(results2014)
		log("Done")
		
		log("Results for TREC Web 2013 topics:")
		results2013TrecFormat.foreach(log)
		log("Results for TREC Web 2014 topics:")
		results2014TrecFormat.foreach(log)
		log("Done")
	}
	
	def buildIndexWriterConfig: IndexWriterConfig = {
		val analyzer = new EnglishAnalyzer()
		val indexWriterConfig = new IndexWriterConfig(analyzer)
		
		val mergePolicy = new TieredMergePolicy()
		val mergeScheduler = new ConcurrentMergeScheduler()
		mergeScheduler.setDefaultMaxMergesAndThreads(false)
		mergeScheduler.disableAutoIOThrottle()
		indexWriterConfig.setMergePolicy(mergePolicy)
		indexWriterConfig.setMergeScheduler(mergeScheduler)
		
		indexWriterConfig.setRAMBufferSizeMB(128d)
		
		indexWriterConfig.setUseCompoundFile(false)
		
		indexWriterConfig.setCommitOnClose(true)
		
		val customCodec = new CustomCodec()
		indexWriterConfig.setCodec(customCodec)
		
		val similarity = new BM25Similarity()
		indexWriterConfig.setSimilarity(similarity)
		
		indexWriterConfig
	}
	
	def getWarcRecordsIterator(dis: DataInputStream): Iterator[WarcRecord] = {
		class WarcRecordsIterator(dis: DataInputStream) extends Iterator[WarcRecord] {
			var record = WarcRecord.readNextWarcRecord(dis)
			closeIfNull()
			
			override def hasNext: Boolean = record != null
			
			override def next(): WarcRecord = {
				val lastRecord = record
				record = WarcRecord.readNextWarcRecord(dis)
				closeIfNull()
				
				lastRecord
			}
			
			def closeIfNull() = if (record == null) dis.close()
		}
		new WarcRecordsIterator(dis)
	}
	
	def search(indexReader: IndexReader, indexSearcher: IndexSearcher, query: DslQuery, maxHits: Int): Array[(String, Double)] = {
		val luceneQuery = query.getLuceneQuery(new EnglishAnalyzer())
		val results = indexSearcher.search(luceneQuery, maxHits).scoreDocs map {
			scoreDoc =>
				val id = indexReader.document(scoreDoc.doc).getField("trecId").stringValue()
				val score = scoreDoc.score.toDouble
				(id, score)
		}
		results
	}
}
