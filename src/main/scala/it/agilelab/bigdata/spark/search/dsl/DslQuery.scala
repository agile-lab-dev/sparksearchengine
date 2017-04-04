package it.agilelab.bigdata.spark.search.dsl

import it.agilelab.bigdata.spark.search.Query
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.index.Term
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.util.{QueryBuilder => LuceneQueryBuilder}
import org.apache.lucene.search.{
	BooleanClause,
	BooleanQuery,
	MatchAllDocsQuery,
	TermQuery,
	Query => LuceneQuery }


abstract class DslQuery extends Query {
	def &&(other: DslQuery): AndQuery = new AndQuery(this, other)

	def ||(other: DslQuery): OrQuery = new OrQuery(this, other)

	def getLuceneQuery(analyzer: Analyzer): LuceneQuery
}

case class AllDocsQuery() extends DslQuery {
	override def getLuceneQuery(analyzer: Analyzer): LuceneQuery = {
		new MatchAllDocsQuery()
	}
}

case class AndQuery(op1: DslQuery, op2: DslQuery) extends DslQuery {
	override def getLuceneQuery(analyzer: Analyzer): LuceneQuery = {
		val builder = new BooleanQuery.Builder()
		builder.add(op1.getLuceneQuery(analyzer), BooleanClause.Occur.MUST)
			.add(op2.getLuceneQuery(analyzer), BooleanClause.Occur.MUST)
			.build()
	}
}

case class OrQuery(op1: DslQuery, op2: DslQuery) extends DslQuery {
	override def getLuceneQuery(analyzer: Analyzer): LuceneQuery = {
		val builder = new BooleanQuery.Builder()
		builder.add(op1.getLuceneQuery(analyzer), BooleanClause.Occur.SHOULD)
			.add(op2.getLuceneQuery(analyzer), BooleanClause.Occur.SHOULD)
			.build()
	}
}

case class NegatedQuery(op: DslQuery) extends DslQuery {
	override def getLuceneQuery(analyzer: Analyzer): LuceneQuery = {
		val builder = new BooleanQuery.Builder()
		builder.add(op.getLuceneQuery(analyzer), BooleanClause.Occur.MUST_NOT).build()
	}

	// rewrite this query to MatchAllDocsQuery AND this, so it works as a top level query
	def rewriteToAllAndThis(analyzer: Analyzer): LuceneQuery = {
		val builder = new BooleanQuery.Builder()
		builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST)
			.add(this.op.getLuceneQuery(analyzer), BooleanClause.Occur.MUST_NOT)
			.build()
	}
}

case class MatchMinQueryExpectsTermSet(field: field, num: Int) {
	def from(termSet: termSet): MatchMinQuery = new MatchMinQuery(field, num, termSet)
}

case class MatchMinQuery(field: field, num: Int, termSet: termSet) extends DslQuery {
	override def getLuceneQuery(analyzer: Analyzer): LuceneQuery =  {
		var builder = new BooleanQuery.Builder()
		for (term <- termSet.terms) {
			builder = builder.add(new TermQuery(new Term(field.name, term.term)), BooleanClause.Occur.SHOULD)
		}
		builder.setMinimumNumberShouldMatch(num)
			.build()
	}
}

case class MatchAllQuery(field:field, termSet: termSet) extends DslQuery {
	override def getLuceneQuery(analyzer: Analyzer): LuceneQuery = {
		var builder = new BooleanQuery.Builder()
		for (term <- termSet.terms) {
			builder = builder.add(new TermQuery(new Term(field.name, term.term)), BooleanClause.Occur.MUST)
		}
		builder.build()
	}
}

case class MatchAnyQuery(field:field, termSet: termSet) extends DslQuery {
	override def getLuceneQuery(analyzer: Analyzer): LuceneQuery = {
		var builder = new BooleanQuery.Builder()
		for (term <- termSet.terms) {
			builder = builder.add(new TermQuery(new Term(field.name, term.term)), BooleanClause.Occur.SHOULD)
		}
		builder.build()
	}
}

case class MatchTermQuery(field: field, term: term) extends DslQuery {
	override def getLuceneQuery(analyzer: Analyzer): LuceneQuery = {
		new TermQuery(new Term(field.name, term.term))
	}
}

case class MatchTextQuery(field: field, text: String) extends DslQuery {
	override def getLuceneQuery(analyzer: Analyzer): LuceneQuery = {
		new LuceneQueryBuilder(analyzer).createBooleanQuery(field.name, text)
	}
}

case class MatchAllTextQuery(field: field, text: String) extends DslQuery {
	override def getLuceneQuery(analyzer: Analyzer): LuceneQuery = {
		new LuceneQueryBuilder(analyzer).createBooleanQuery(field.name, text, BooleanClause.Occur.MUST)
	}
}

case class MatchPhraseQuery(field: field, phrase: String) extends DslQuery {
	override def getLuceneQuery(analyzer: Analyzer): LuceneQuery = {
		new LuceneQueryBuilder(analyzer).createPhraseQuery(field.name, phrase)
	}
}

// this parses the text as if it were a query in the Lucene query syntax
// expensive because it instantiates a new QueryParser for each query, as they are not thread-safe
// if at all possible, just use the DSL
case class ParsedQuery(field: field, query: String) extends DslQuery {
	override def getLuceneQuery(analyzer: Analyzer): LuceneQuery = {
		new QueryParser(field.name, analyzer).parse(query)
	}
}