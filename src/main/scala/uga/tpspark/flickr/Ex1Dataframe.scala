package uga.tpspark.flickr

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType
import java.net.URLDecoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.Encoders

object Ex1Dataframe {
  def main(args: Array[String]): Unit = {
    println("hello")
    var spark: SparkSession = null
    try {
      spark = SparkSession.builder().appName("Flickr using dataframes").getOrCreate()

      //   * Photo/video identifier
      //   * User NSID
      //   * User nickname
      //   * Date taken
      //   * Date uploaded
      //   * Capture device
      //   * Title
      //   * Description
      //   * User tags (comma-separated)
      //   * Machine tags (comma-separated)
      //   * Longitude
      //   * Latitude
      //   * Accuracy
      //   * Photo/video page URL
      //   * Photo/video download URL
      //   * License name
      //   * License URL
      //   * Photo/video server identifier
      //   * Photo/video farm identifier
      //   * Photo/video secret
      //   * Photo/video secret original
      //   * Photo/video extension original
      //   * Photos/video marker (0 = photo, 1 = video)

      val customSchemaFlickrMeta = StructType(Array(
        StructField("photo_id", LongType, true),
        StructField("user_id", StringType, true),
        StructField("user_nickname", StringType, true),
        StructField("date_taken", StringType, true),
        StructField("date_uploaded", StringType, true),
        StructField("device", StringType, true),
        StructField("title", StringType, true),
        StructField("description", StringType, true),
        StructField("user_tags", StringType, true),
        StructField("machine_tags", StringType, true),
        StructField("longitude", FloatType, false),
        StructField("latitude", FloatType, false),
        StructField("accuracy", StringType, true),
        StructField("url", StringType, true),
        StructField("download_url", StringType, true),
        StructField("license", StringType, true),
        StructField("license_url", StringType, true),
        StructField("server_id", StringType, true),
        StructField("farm_id", StringType, true),
        StructField("secret", StringType, true),
        StructField("secret_original", StringType, true),
        StructField("extension_original", StringType, true),
        StructField("marker", ByteType, true)))
      
      val originalFlickrMeta = spark.sqlContext.read
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .schema(customSchemaFlickrMeta)
        .load("flickrSample.txt")
      
      // Question 1.1
      originalFlickrMeta.createOrReplaceTempView("flickrMeta")
      val someFileds = spark.sql("select photo_id, longitude, latitude, license from flickrMeta")
      // someFileds.show()
      
      // Question 1.2
      val interestingPictures = spark.sql("select * from flickrMeta where license is not NULL and longitude <> -1 and latitude <> -1")
      val interestingPicturesCount = interestingPictures.count()
      println(s"interestingPicturesCount = $interestingPicturesCount")

      // Question 1.3
      interestingPictures.explain()

      // Question 1.4
      interestingPictures.show()

      // Question 1.5
      val flickrLicenseMeta = spark.sqlContext.read
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "true")
        .load("FlickrLicense.txt")

      flickrLicenseMeta.createOrReplaceTempView("flickrLicenseMeta")
      // flickrLicenseMeta.show()
      val interestingPicturesND = spark.sql("select * from flickrMeta join flickrLicenseMeta on flickrMeta.license = flickrLicenseMeta.name where NonDerivative = 1")
      interestingPicturesND.explain()
      interestingPicturesND.show()

      val interestingPicturesNDCount = interestingPicturesND.count()
      println(s"interestingPicturesNDCount = $interestingPicturesNDCount")

      // Question 1.6
      interestingPictures.cache()

      val interestingPicturesCached = spark.sql("select * from flickrMeta where license is not NULL and longitude <> -1 and latitude <> -1")
      // interestingPictures.explain()
      interestingPicturesCached.explain()

      // flickrLicenseMeta.show()


      // val notInterestingPoints = spark.sql("select license from flickrMeta")
      // val notInterestingPointsCount = notInterestingPoints.count()

      // println(s"interestingPointsCount, $interestingPointsCount")
      // println(s"notInterestingPointsCount, $notInterestingPointsCount")

    } catch {
      case e: Exception => throw e
    } finally {
      spark.stop()
    }
    println("done")
  }
}