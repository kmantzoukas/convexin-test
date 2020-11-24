import ConvexInUtilities.{aggregateValuesForKeys, prepareAndCleanUpData, readDataFromS3, writeDataToS3}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object ConvexInTestAssignment {

  def main(args: Array[String]): Unit = {
    args match {
      case Array(master, ipath, opath) => executePipeline(master, ipath, opath)
      case _ => println("The list of parameters is not correct.")
    }
  }

  def executePipeline(master: String, ipath: String, opath: String): Unit = {

    implicit val spark: SparkSession = SparkSession.builder.master(master).getOrCreate
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider",
      "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

    /*
    readDataFromS3(ipath) match {
      case Success(data) => {
        val preparedAndClearedUpData = prepareAndCleanUpData(data)
        preparedAndClearedUpData match {
          case Success(data) => {
            val aggregatedData = aggregateValuesForKeys(data)
            aggregatedData match {
              case Success(data) => {
                writeDataToS3(data, opath) match {
                  case Success(_) => println("Results file has been created successfully.")
                  case Failure(e) => println(s"An error occurred while saving the result file in $opath. Error: ${e.getMessage}")
                }
              }
              case Failure(e) => println(s"An error occurred while aggregating the data. Error: ${e.getMessage}")
            }
          }
          case Failure(e) => println(s"An error occurred while cleaning up the data. Error: ${e.getMessage}")
        }
      }
      case Failure(e) => println(s"Unable to load file(s) from path $ipath with error: ${e.getMessage}")
    }
    */

    /*
    readDataFromS3(ipath).flatMap{
      dataLoaded =>
        prepareAndCleanUpData(dataLoaded).flatMap{
          preparedAndClearedUpData =>
            aggregateValuesForKeys(preparedAndClearedUpData).flatMap{
              aggregatedData => writeDataToS3(aggregatedData, opath)
            }
        }
    }
    */

    val res: Try[Unit] =
      for {
        loadedData <- readDataFromS3(ipath)
        cleanedUpData <- prepareAndCleanUpData(loadedData)
        aggregatedData <- aggregateValuesForKeys(cleanedUpData)
        result <- writeDataToS3(aggregatedData, opath)
      } yield result

    res match {
      case Success(_) => println("Result tsv file has been successfully created")
      case Failure(e) => println(s"Result tsv file has not been created with error: ${e.getMessage}")
    }

    spark.stop
  }
}