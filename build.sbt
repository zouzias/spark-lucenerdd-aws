organization := "org.zouzias"
name := "spark-lucenerdd-aws"
scalaVersion := "2.11.12"
val sparkV = "2.3.1"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

resolvers += "Apache Repos" at "https://repository.apache.org/content/repositories/releases"
resolvers += "OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies ++= Seq(
	"org.zouzias" %% "spark-lucenerdd" % version.value,
	"joda-time"   % "joda-time"					% "2.10",
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