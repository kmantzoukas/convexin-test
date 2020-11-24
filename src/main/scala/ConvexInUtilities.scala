import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object ConvexInUtilities {

  private def zeroIfEmpty(s: String): Int = if (s.trim.isEmpty) 0 else s.toInt

  private def areAllDigits(x: String) = x forall Character.isDigit

  def readDataFromS3(inputPath: String)(implicit spark: SparkSession): Try[RDD[String]] = {
    try {
      Success(spark.sparkContext.textFile(s"$inputPath/*.*sv"))
    } catch {
      case e: Throwable => Failure(e)
    }
  }

  def prepareAndCleanUpData(data: RDD[String]) = {
    try {
      Success(data.map(record => {
        val key = record.split(",|\t+")(0)
        val value = record.split(",|\t+")(1)
        (key, value)
      }).
        filter { case (key, value) => areAllDigits(key) && areAllDigits(value) }.
        map { case (key, value) => ((zeroIfEmpty(key), zeroIfEmpty(value)), 1) })
    } catch {
      case e: Throwable => Failure(e)
    }
  }

  def aggregateValuesForKeys(data: RDD[((Int, Int), Int)]): Try[RDD[(Int, Int)]] = {
     try{
       Success(data.reduceByKey(_ + _).filter { case (key, counter) => (counter % 2) != 0 }.map { case (key, _) => key })
     } catch {
       case e: Throwable => Failure(e)
     }
  }

  def writeDataToS3(data: RDD[(Int, Int)], outputFile: String)(implicit spark: SparkSession): Try[Unit] = {
    try {
      Try(spark.
        createDataFrame(data).
        coalesce(1). // write the results on a single file for convenience - might not be appropriate on production
        write.option("header", "false").
        option("delimiter", "\t").
        mode("overwrite").
        csv(outputFile))
    } catch {
      case e: Throwable => Failure(e)
    }
  }
}
