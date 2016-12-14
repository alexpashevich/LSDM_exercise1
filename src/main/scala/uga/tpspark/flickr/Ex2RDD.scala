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
import org.apache.spark.rdd.RDD

object Ex2RDD {
  def main(args: Array[String]): Unit = {
    println("hello")
    var spark: SparkSession = null
    try {
      spark = SparkSession.builder().appName("Flickr using dataframes").getOrCreate()
      val originalFlickrMeta: RDD[String] = spark.sparkContext.textFile("flickrSample.txt")

      /* Ex. 1 */
      val first5 = originalFlickrMeta.take(5)
      val count = originalFlickrMeta.count()

      println("Ex. 1: (first 5 rows) " + first5.mkString("\t"));  
      println("Ex. 1: (count) " + count)

      /*  Ex. 2 */
      val pictures: RDD[Picture] = originalFlickrMeta.map(row => new Picture(row.split("\t")))
      val interstingPics: RDD[Picture] = pictures.filter(pic => pic.hasValidCountry & pic.hasTags)
      println("Ex. 2: (interesting pics) " + interstingPics.take(5).mkString("\t"))
      println("Ex. 2: (count) " + interstingPics.count())    
        
      /*  Ex. 3 */
      val groupByCountry = interstingPics.groupBy(pic => pic.c)
      // groupByCountry: [(Country, Iterable[Picture])]
      println("Ex. 3: (country) " + groupByCountry.first()._1)
      groupByCountry.first()._2.foreach(pic => println(pic.toString()))

      /* Ex. 4 */
      val countriesTags = groupByCountry.map(x => (x._1, x._2.flatMap(pic => pic.userTags)))
      // groupByCountry: [(Country, Iterable[Picture])]
      println("Ex. 4: (country) " + countriesTags.first().toString()) 

      /* Ex. 5 */
      val tagsCount = countriesTags.map(x => (x._1, x._2.groupBy(identity).mapValues(_.size).map(identity)))
      // groupByCountry: [(Country, Map[String, Int])]
      println("Ex. 5: (country) " + tagsCount.first().toString())


    } catch {
      case e: Exception => throw e
    } finally {
      spark.stop()
    }
    println("done")
  }
}