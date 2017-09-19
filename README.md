[![Gitter chat](https://badges.gitter.im/spark-search.png)](https://gitter.im/spark-search/)

# SearchableRDD for Apache Spark
#### Big Data search with Spark and Lucene

**spark-search** is an open source library for [Apache Spark](http://spark.apache.org/) that allows you to easily index and search your Spark datasets with similar functionality to that of a dedicated search engine like Elasticsearch or Solr.

With spark-search you can leverage information retrieval functionality to analyze and explore you Spark datasets without having to setup an external search engine, lowering the effort needed. Without external systems there are no deployment, administration or resource costs associated with them; everything needed for information retrieval is handled inside your Spark application.
 
Unstructured information like text is easy to leverage by using the standard query types for full-text search; filters for efficient interrogations are provided for non-textual data types. Queries and filters can be mixed together to express complex information retrieval needs.

With a transparent integration with Spark's `RDD`s and a domain specific language for queries and filters the effort needed to leverage information retrieval from Spark is brought to a minimum.

## Setup

#### SBT

Add the repository to your resolvers:

```sbtshell
resolvers += Resolver.bintrayRepo("agile-lab-dev", "SparkSearchEngine")
```

Add the dependency:

```sbtshell
libraryDependencies += "it.agilelab" %% "spark-search" % "0.1"
```

## Documentation

The scaladoc is available at:
- [https://agile-lab-dev.github.io/sparksearchengine/scaladoc/0.1/scala_2.10] for Scala 2.10
- [https://agile-lab-dev.github.io/sparksearchengine/scaladoc/0.1/scala_2.11] for Scala 2.11

## How it works

Powered by [Apache Lucene](http://lucene.apache.org/), `spark-search` enables you to run queries on `RDD`s by building Lucene indices for the elements in your input `RDD`s, creating `SearchableRDD`s which you can then execute queries on.

The only requirement is that elements in the input `RDD` must implement the `Indexable` trait. There is an experimental automatic conversion feature which allows you to transparently use your case classes without any work, which currently only works in the Scala 2.11 build. When used, spark-search will automatically add the functionality needed to implement the trait by using reflection and runtime code-generation. See the scaladoc for `it.agilelab.bigdata.spark.search.Indexable` and `it.agilelab.bigdata.spark.search.Indexable.ProductAsIndexable` for further information.

Queries can be specified either with the Lucene syntax or with `spark-search`'s own domain specific language; to explore the DSL, check out the scaladoc for the `it.agilelab.bigdata.spark.search.dsl.QueryBuilder` class.


## Example: indexing and searching a Wikipedia dump

As a usage example, let's index and search a Wikipedia dump; let's start with the Simple English Wikipedia, as it is small enough to be readily downloadable and usable on less powerful hardware.

Head over to [https://dumps.wikimedia.org/simplewiki/] and grab the latest dump; choose the one marked as "Articles, templates, media/file descriptions, and primary meta-pages, in multiple bz2 streams, 100 pages per stream" - it should be named something like `simplewiki-20170820-pages-articles-multistream.xml.bz2`. Download it and decompress it somewhere.

First, we parse the XML dump into and `RDD[wikipage]`:

```scala
import it.agilelab.bigdata.spark.search.utils.WikipediaXmlDumpParser.xmlDumpToRdd
import it.agilelab.bigdata.spark.search.utils.wikipage

// path to xml dump
val xmlPath = "/path/to/simplewiki-20170820-pages-articles-multistream.xml"

// read xml dump into an rdd of wikipages
val wikipages = xmlDumpToRdd(sc, xmlPath).cache()
```

We now check how many pages we got:

```scala
println(s"Number of pages: ${wikipages.count()}")
```

Let's make it a `SearchableRDD`:

```scala
import it.agilelab.bigdata.spark.search.SearchableRDD
import it.agilelab.bigdata.spark.search.dsl._
import it.agilelab.bigdata.spark.search.impl.analyzers.EnglishWikipediaAnalyzer
import it.agilelab.bigdata.spark.search.impl.queries.DefaultQueryConstructor
import it.agilelab.bigdata.spark.search.impl.{DistributedIndexLuceneRDD, LuceneConfig}

// define a configuration to use english analyzers for wikipedia and the default query constructor
val luceneConfig = LuceneConfig(classOf[EnglishWikipediaAnalyzer],

// index using DistributedIndexLuceneRDD implementation with 2 indices
val searchable: SearchableRDD[wikipage] = DistributedIndexLuceneRDD(wikipages, 2, luceneConfig).cache()
```

We can now do queries:

```scala
// define a query using the DSL
val query = "text" matchAll termSet("island")

// run it against the searchable rdd
val queryResults = searchable.aggregatingSearch(query, 10)

// print results
println(s"Results for query $query:")
queryResults foreach { result => println(f"\tscore: ${result._2}%6.3f title: ${result._1.title}") }
```

Get information about the indices that were built:

```scala
val indicesInfo = searchable.getIndicesInfo

// print it
println(indicesInfo.prettyToString())
```

Get information about the terms:

```scala
val termInfo = searchable.getTermCounts

// print top 10 terms for "title" field
val topTenTerms = termInfo("title").toList.sortBy(_._2).reverse.take(10)
println("Top 10 terms for \"title\" field:")
topTenTerms foreach { case (term, count) => println(s"\tterm: $term count: $count") }
```

Or do a query join to find similar pages:

```scala
// define query generator where we simply use the title and the first few characters of the text as a query
val queryGenerator: wikipage => DslQuery = (wp) => "text" matchText (wp.title + wp.text.take(200))

// do a query join on itself
val join = searchable.queryJoin(searchable, queryGenerator, 5) map {
    case (wp, results) => (wp, results map { case (wp2, score) => (wp2.title, score) })
}
val queryJoinResults = join.take(5)

// print first five elements and corresponding matches
println("Results for query join:")
queryJoinResults foreach {
    case (wp, results) =>
        println(s"title: ${wp.title}")
        results foreach { result => println(f"\tscore: ${result._2}%6.3f title: ${result._1}") }
}
```

You can find this example in `it.agilelab.bigdata.spark.search.examples.SearchableRDDExamples`, ready to be run with spark-submit.
