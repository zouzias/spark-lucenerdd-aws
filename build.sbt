organization := "org.zouzias"
name := "spark-lucenerdd-aws"
version := "0.0.24"
scalaVersion := "2.10.6"
val sparkV = "1.6.2"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7", "-Xlint")

libraryDependencies ++= Seq(
	"org.zouzias" %% "spark-lucenerdd" % version.value,
	"joda-time"   % "joda-time"					% "2.9.4",
	"org.apache.spark" %% "spark-core" % sparkV % "provided",
	"org.apache.spark" %% "spark-sql" % sparkV % "provided" ,
	"org.scala-lang"    % "scala-library" % scalaVersion.value % "compile"
)

lazy val root = (project in file(".")).
	enablePlugins(BuildInfoPlugin).
	settings(
		buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
		buildInfoPackage := "org.zouzias.spark.lucenerdd.aws"
	)
