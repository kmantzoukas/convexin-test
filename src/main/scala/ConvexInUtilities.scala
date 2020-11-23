import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ConvexInUtilities {

  private def zeroIfEmpty(s:String):Int = if(s.trim.isEmpty) 0 else s.toInt

  private def areAllDigits(x: String) = x forall Character.isDigit

def readDataFromS3(inputPath: String)(implicit spark: SparkSession) =
  spark.sparkContext.textFile(s"$inputPath/*.*sv") // load all files with suffix *.*sv e.g. file-1.csv, file-2.tsv, etc.

def prepareAndCleanUpData(data: RDD[String]) =
  data.
    map(record => {
      val key = record.split(",|\t+")(0)
      val value = record.split(",|\t+")(1)
      (key, value)
    }).
    filter{ case (key, value) => areAllDigits(key) && areAllDigits(value)}. // filter out the headers
    map{case (key, value) => ((zeroIfEmpty(key), zeroIfEmpty(value)), 1)} // if the keys/values are empty we will use zero

def aggregateValuesForKeysNaive(data: RDD[((Int, Int), Int)]) =
  data.
    reduceByKey(_ + _).
    filter{case (key, counter) => (counter % 2) != 0}.
    map{case (key, _) => key} // dismiss the number of occurrences

def writeDataToS3(data: RDD[(Int, Int)], outputFile: String)(implicit spark: SparkSession) =
  spark.
    createDataFrame(data).
    coalesce(1). // write the results on a single file for convenience - might not be appropriate on production
    write.option("header", "false").
    option("delimiter", "\t").
    mode("overwrite").
    csv(outputFile)

}
