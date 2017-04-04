package it.agilelab.bigdata.spark.search


import java.util.Date

import scala.collection.mutable
import scala.collection.mutable.ArrayBuilder
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

import it.agilelab.bigdata.spark.search.utils.Logging

trait Storeable[T] extends Indexable {
	def getData: T
	// default impl? identity? how t avoid having to specify generic? two indexable traits one extending the other?
}

/**
	* An indexable element.
	*/
trait Indexable {
	def getFields: Iterable[Field[_]]

	def getFields(prefix: String): Iterable[Field[_]] = {
		getFields map {
			case s: StringField => StringField(prefix + s.name, s.value)
			case nps: NoPositionsStringField => NoPositionsStringField(prefix + nps.name, nps.value)
			case i: IntField => IntField(prefix + i.name, i.value)
			case l: LongField => LongField(prefix + l.name, l.value)
			case f: FloatField => FloatField(prefix + f.name, f.value)
			case d: DoubleField => DoubleField(prefix + d.name, d.value)
			case dt: DateField => DateField(prefix + dt.name, dt.value)
			case sq: SeqField => SeqField(prefix + sq.name, sq.value)
		}
	}
	
	
}
object Indexable extends Logging {
	val S   = typeOf[String]
	val I   = typeOf[Int]
	val L   = typeOf[Long]
	val F   = typeOf[Float]
	val D   = typeOf[Double]
	val Dt  = typeOf[Date]
	val Sq  = typeOf[Seq[_]]
	val Ind = typeOf[Indexable]
	val P   = typeOf[Product]
	val prefixSeparator = '.'
	val generatedFunctions: mutable.Map[String, (Product, String) => Iterable[Field[_]]] = mutable.Map()

	/**
		* Allows to view a `Product` as an `Indexable`; for each declared field of the `Product`,
		* `Field.name` is the declared name, `Field.value` is its value.
		* It is recursive: `Indexable` values are converted to multiple fields; their `Field.name`s are
		* prefixed with the `Product` field name plus a dot, their `Field.value` is their value.
		* Supported field types:
		* `String`, `Int`, `Long`, `Float`, `Double` (primitive types)
		* `Product`, `Indexable`
		* If a field's type is not supported, the value is the String returned by its `toString()` method.
		*
		* DOES NOT WORK PROPERLY ON CYCLIC OBJECT GRAPHS.
		*
		* @param product  The wrapped product.
		* @tparam T
		*/
	implicit class ProductAsIndexable[T <: Product : TypeTag : ClassTag](product: T) extends Indexable {
		val className = product.getClass.getName

		override def getFields: Iterable[Field[_]] = getFields("")

		override def getFields(prefix: String): Iterable[Field[_]] = {
			// get function
			val function = Indexable.synchronized {
				// get if function is available, generate and get otherwise
				generatedFunctions getOrElseUpdate (className, generateFunction(product))
			}
			// use
			function(product, prefix)
		}

		private def generateFunction(product: Any): (Product, String) => Iterable[Field[_]] = {
			val start = System.nanoTime()
			
			// get type
			val tpe = typeTag[T].tpe
			logger.info("Generating code for type {}", tpe)
			
			// prepare mirrors
			val rm = universe.runtimeMirror(product.getClass.getClassLoader)
			val im = rm.reflect(product)
			// prepare lists for generic type substitution
			val TypeRef(_, _, tpeTypeArgs) = tpe
			val from = tpe.typeSymbol.asType.typeParams
			val to = tpeTypeArgs
			// get accessor methods
			val members = tpe.members
			val accessors = members filter (m => m.isMethod && m.asInstanceOf[MethodSymbol].isAccessor && !m.isPrivate)

			// codegen 1/3: generate field generation statements
			logger.info("Generating field generation statements for type {}", tpe)
			val stats = new ArrayBuilder.ofRef[Tree]()
			for (accessor <- accessors) {
				val method = accessor.asMethod
				// prepare method mirror
				val mm = im.reflectMethod(method)
				// get name and return type
				val name = mm.symbol.name.decoded
				val valueType = method.returnType.substituteTypes(from, to)
				// create statement
				valueType match {
					case s if s =:= S    => stats += q"""fields += StringField(prefix + $name, product.${name: TermName})"""
					case i if i =:= I    => stats += q"""fields += IntField(prefix + $name, product.${name: TermName})"""
					case l if l =:= L    => stats += q"""fields += LongField(prefix + $name, product.${name: TermName})"""
					case f if f =:= F    => stats += q"""fields += FloatField(prefix + $name, product.${name: TermName})"""
					case d if d =:= D    => stats += q"""fields += DoubleField(prefix + $name, product.${name: TermName})"""
					case dt if dt =:= Dt => stats += q"""fields += DateField(prefix + $name, product.${name: TermName})"""
					case sq if sq =:= Sq => stats += q"""fields += SeqField(prefix + $name, product.${name: TermName})"""
					case x if x <:< Ind  => stats += q"""fields ++= product.${name: TermName}.getFields(prefix + $name + $prefixSeparator)""" // get fields for indexables
					case p if p <:< P    => stats += q"""fields ++= product.${name: TermName}.getFields(prefix + $name + $prefixSeparator)""" // get fields through implicit conversion for products
					case _               => stats += q"""fields += StringField(prefix + $name, product.${name: TermName}.toString)""" // fallback for unknown types
				}
			}
			val statements = stats.result().toList
			// codegen 2/3: generate surrounding code, splice in field generation statements
			logger.info("Generating complete code for type {}", tpe)
			val code =
				q"""import it.agilelab.bigdata.spark.search.Indexable
						import it.agilelab.bigdata.spark.search.Indexable._
						import it.agilelab.bigdata.spark.search.Field
						import it.agilelab.bigdata.spark.search.StringField
						import it.agilelab.bigdata.spark.search.IntField
						import it.agilelab.bigdata.spark.search.LongField
						import it.agilelab.bigdata.spark.search.FloatField
						import it.agilelab.bigdata.spark.search.DoubleField
						import it.agilelab.bigdata.spark.search.DateField
						import it.agilelab.bigdata.spark.search.SeqField
						import scala.collection.mutable.ArrayBuilder
						def getFields(anyProduct: Any, prefix: String): Iterable[Field[_]] = {
							val product = anyProduct.asInstanceOf[$tpe]
							val fields = new ArrayBuilder.ofRef[Field[_]]()
				      fields.sizeHint(${accessors.size})
							..$statements
				      fields.result()
						}
				    getFields _"""
			logger.debug("Generated code for type {}:\n{}", tpe: Any, code: Any) // ascriptions are a workaround for scala failing to resolve overloaded methods
			// codegen 3/3: compile
			logger.info("Compiling code code for type {}", tpe)
			val toolbox = rm.mkToolBox()
			val compiledCode = toolbox.compile(code)
			
			val end = System.nanoTime()
			logger.info("Code for type {} generated in {}s", tpe, (end-start)/1000000000d)

			// cast & return
			compiledCode().asInstanceOf[(Product, String) => Iterable[Field[_]]]
		}
	}
}