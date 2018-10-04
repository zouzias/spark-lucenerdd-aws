// Thanks to https://medium.com/@mrpowers/working-with-s3-and-spark-locally-1374bb0a354
import org.apache.spark.SparkContext
//val sc = SparkContext.getOrCreate()
val accessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
spark.sparkContext.hadoopConfiguration.set("com.amazonaws.services.s3.enableV4", "true")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsAccessKeyId", accessKeyId)
spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsSecretAccessKey", secretAccessKey)
spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3-eu-central-1.amazonaws.com")
// hadoop_conf.set("fs.s3a.endpoint", "s3." + aws_region + ".amazonaws.com")
// sc.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
//
//


// https://medium.com/@subhojit20_27731/apache-spark-and-amazon-s3-gotchas-and-best-practices-a767242f3d98
import com.amazonaws.services.s3._, model._
import com.amazonaws.auth.BasicAWSCredentials

val request = new ListObjectsRequest()
val bucket = "spark-lucenerdd"
val prefix = "datasets/deduplication"
val pageLength = 5
request.setBucketName(bucket)
request.setPrefix(prefix)
request.setMaxKeys(pageLength)
def s3 = new AmazonS3Client(new BasicAWSCredentials(accessKeyId, secretAccessKey))

val objs = s3.listObjects(request) // Note that this method returns truncated data if longer than the "pageLength" above. You might need to deal with that.
sc.parallelize(objs.getObjectSummaries.map(_.getKey).toList)
  .flatMap { key => Source.fromInputStream(s3.getObject(bucket, key).getObjectContent: InputStream).getLines }
