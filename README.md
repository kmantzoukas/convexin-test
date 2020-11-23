## ConvexIn assignment

### Implementation details

The implementation has been broken into the following steps:
* Read the data from the input folder - *__readDataFromS3__*( )
* Clear up and prepare the data to facilitate the aggregation step - *__prepareAndCleanUpData__*( )
* Aggregate the data by key - *__aggregateValuesForKeysNaive__*( )
* Write the data to S3 - *__writeDataToS3__*( )

### Notes

* We have used *__reduceByKey__*( ) to group the data which is more efficient compared to *__groupByKey__*( )

* __AWS_ACCESS_KEY_ID__ and __AWS_SECRET_ACCESS_KEY__ have to be set as environment variables

* The application takes 3 arguments namely the maser url, the input path and the output path. To run this in local mode from within sbt, one should type the following command:

`run local[*] s3a://convexin-data/input/ s3a://convexin-data/output/results.tsv`

1. Spark master url: `local[*]`
2. S3 input path: `s3a://convexin-data/input/` 
3. S3 output path: `s3a://convexin-data/output/results.tsv`