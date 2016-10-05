organization := "org.zouzias"
name := "spark-lucenerdd-aws"
version := "0.2.0"
scalaVersion := "2.11.8"
val sparkV = "2.0.0"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

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
