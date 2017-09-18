import sbt.Keys._

name := "spark-search"
version := "0.1"
organization := "it.agilelab"
organizationHomepage := Some(url("http://www.agilelab.it"))
homepage := Some(url("http://www.agilelab.it"))
licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")))

/* scala */
scalacOptions += "-feature"
scalacOptions += "-deprecation"
crossScalaVersions := Seq("2.10.6", "2.11.11")

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
libraryDependencies ++= (
  if (scalaBinaryVersion.value == "2.10") // add only if scala 2.10; in 2.11 quasiquotes are built-in
    Seq("org.scalamacros" %% "quasiquotes" % "2.1.0")
  else
    Seq[ModuleID]()
)
autoCompilerPlugins := true
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full) // use CrossVersion.full so the entire scala version is used in artifact name
// kryo
libraryDependencies += "com.esotericsoftware.kryo" % "kryo" % "2.24.0"
// spark-xml
libraryDependencies += "com.databricks" %% "spark-xml" % "0.3.2"
// elasticsearch
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark" % "5.0.0-alpha4" % "provided"

/* scaladoc */
// configure root documentation
scalacOptions in (Compile, doc) ++= Seq("-doc-root-content", baseDirectory.value + "/root-doc.txt")
// enable groups
scalacOptions in (Compile, doc) ++= Seq("-groups")
// mappings for dependencies' docs
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

/* bintray publishing */
bintrayOrganization := Some("agile-lab-dev") // organization
bintrayRepository := "SparkSearchEngine" // target repo
bintrayPackage := "it.agilelab" // target package
bintrayReleaseOnPublish := false // do not automatically release, instead do sbt publish, then sbt bintrayRelease
