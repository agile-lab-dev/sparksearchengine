import sbt.Keys._

name := "SparkSearchEngine"
version := "0.1-SNAPSHOT"

/* scala */
scalaVersion := "2.10.6" // note: hardcoded scala version in addCompilerPlugin!
scalacOptions += "-feature"
scalacOptions += "-deprecation"

/* dependencies */
// spark
val sparkVersion = "1.6.2"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
// lucene
val luceneVersion = "6.0.0"
libraryDependencies += "org.apache.lucene" % "lucene-core" % luceneVersion
libraryDependencies += "org.apache.lucene" % "lucene-analyzers-common" % luceneVersion
libraryDependencies += "org.apache.lucene" % "lucene-queryparser" % luceneVersion
libraryDependencies += "org.apache.lucene" % "lucene-codecs" % luceneVersion
// tyesafe config
libraryDependencies += "com.typesafe" % "config" % "1.3.0"
// macros & quasiquotes
libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value
libraryDependencies += "org.scalamacros" %% "quasiquotes" % "2.1.0"
autoCompilerPlugins := true
addCompilerPlugin("org.scalamacros" % "paradise_2.10.6" % "2.1.0")
// kryo
libraryDependencies += "com.esotericsoftware.kryo" % "kryo" % "2.24.0"
// spark-xml
libraryDependencies += "com.databricks" %% "spark-xml" % "0.3.2"
// jsoup
libraryDependencies += "org.jsoup" % "jsoup" % "1.10.1"
// elasticsearch
libraryDependencies += "org.elasticsearch" % "elasticsearch-spark_2.10" % "5.0.0-alpha4" % "provided"

/* scaladoc */
scalacOptions in (Compile, doc) ++= Seq("-doc-root-content", baseDirectory.value + "/root-doc.txt", "-groups")
autoAPIMappings := true
apiMappings ++= {
  val cp: Seq[Attributed[File]] = (fullClasspath in Compile).value
  def findManagedDependency(organization: String, name: String): File = {
    val jars = for {
      entry <- cp
      module <- entry.get(moduleID.key)
      if module.organization == organization
      if module.name.startsWith(name)
      jarFile = entry.data
    } yield jarFile
    jars.head
  }
  Map(
    findManagedDependency("org.apache.spark","spark-core") -> url(s"http://spark.apache.org/docs/$sparkVersion/api/scala/index.html")
  )
}

/* uber jar */
assemblyJarName in assembly := "sparksearchengine-all.jar"

