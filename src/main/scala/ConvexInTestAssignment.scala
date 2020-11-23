import ConvexInUtilities.{aggregateValuesForKeysNaive, prepareAndCleanUpData, readDataFromS3, writeDataToS3}
import org.apache.spark.sql.SparkSession

object ConvexInTestAssignment {

  def main(args: Array[String]): Unit = {
    args match {
      case Array(master, ipath, opath) => executePipeline(master, ipath, opath)
      case _  => println("The list of parameters is not correct.")
    }
  }

  def executePipeline(master:String, ipath:String, opath:String) = {
    implicit val spark = SparkSession.builder.master(master).getOrCreate

    // requires the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables to be set
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

    val data = readDataFromS3(ipath)
    val preparedAndClearedUpData = prepareAndCleanUpData(data)
    val aggregatedData = aggregateValuesForKeysNaive(preparedAndClearedUpData)
    writeDataToS3(aggregatedData, opath)

    spark.stop
  }

}