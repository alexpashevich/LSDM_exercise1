# Spark - Practical work
Authors: Pashevich Alexander & Kupiec Marcin

## Processing data using the DataFrame API

### Question 1
To select some fields using Spark an SQL command can be used:
```java
originalFlickrMeta.createOrReplaceTempView("flickrMeta")
val someFileds = spark.sql("select photo_id, longitude, latitude, license from flickrMeta")
```
### Question 2
The conditional clause can be used as usually:
```java
val interestingPictures = spark.sql("select * from flickrMeta where license is not NULL and longitude <> -1 and latitude <> -1")
```
### Question 3
Spark does not immediately executes the commands but do it in a lazy way meaning that the commands are executed only when the result is asked by user. However Spark can show the execution plan which will be used:
```java
interestingPictures.explain()
```
We can see that the csv file is scanned using some filter options:
```java
== Physical Plan ==
*Project [photo_id#0L, user_id#1, user_nickname#2, date_taken#3, date_uploaded#4, device#5, title#6, description#7, user_tags#8, machine_tags#9, longitude#10, latitude#11, accuracy#12, url#13, download_url#14, license#15, license_url#16, server_id#17, farm_id#18, secret#19, secret_original#20, extension_original#21, marker#22]
+- *Filter ((((isnotnull(longitude#10) && isnotnull(latitude#11)) && isnotnull(license#15)) && NOT (longitude#10 = -1.0)) && NOT (latitude#11 = -1.0))
   +- *Scan csv [photo_id#0L,user_id#1,user_nickname#2,date_taken#3,date_uploaded#4,device#5,title#6,description#7,user_tags#8,machine_tags#9,longitude#10,latitude#11,accuracy#12,url#13,download_url#14,license#15,license_url#16,server_id#17,farm_id#18,secret#19,secret_original#20,extension_original#21,marker#22] Format: CSV, InputPaths: file:/Users/alexpashevich/MoSIG/LSDM/TpFlickrSkeleton/flickrSample.txt, PartitionFilters: [], PushedFilters: [IsNotNull(longitude), IsNotNull(latitude), IsNotNull(license), Not(EqualTo(longitude,-1.0)), Not..., ReadSchema: struct<photo_id:bigint,user_id:string,user_nickname:string,date_taken:string,date_uploaded:string...
```
### Question 4
The show() command will display content in the console output:
```java
interestingPictures.show()
```
We do not show the output as it is too clumsy.
### Question 5
We load a DataFrame with properties of licenses and make a join with the pictures DataFrame to get only pictures with NonDerivative licenses.
```java
val flickrLicenseMeta = spark.sqlContext.read
        					 .format("csv")
        					 .option("delimiter", "\t")
        					 .option("header", "true")
        					 .load("FlickrLicense.txt")
flickrLicenseMeta.createOrReplaceTempView("flickrLicenseMeta")
val interestingPicturesND = spark.sql("select * from flickrMeta join flickrLicenseMeta on flickrMeta.license = flickrLicenseMeta.name where flickrMeta.license is not NULL and flickrMeta.longitude <> -1 and flickrMeta.latitude <> -1 and flickrLicenseMeta.NonDerivative = 1")
interestingPicturesND.explain()
interestingPicturesND.show()
```
### Question 6
To make execution more efficient we can cache a query result in the memory:
```java
interestingPictures.registerTempTable("interestingPictures")
spark.sqlContext.cacheTable("interestingPictures")
```
Then we execute the same command as before:
```java
val interestingPicturesNDWithCache = spark.sql("select * from interestingPictures join flickrLicenseMeta on interestingPictures.license = flickrLicenseMeta.name")
interestingPicturesNDWithCache.explain()
```
If we take a look at the output of the explain() function, we will notice that now the text file is not scanned anymore to get the intersting pictures. Instead an inMemoryRelation is used:
```java
== Physical Plan ==
*BroadcastHashJoin [license#15], [name#182], Inner, BuildRight
:- *Filter isnotnull(license#15)
:  +- InMemoryTableScan [photo_id#0L, user_id#1, user_nickname#2, date_taken#3, date_uploaded#4, device#5, title#6, description#7, user_tags#8, machine_tags#9, longitude#10, latitude#11, accuracy#12, url#13, download_url#14, license#15, license_url#16, server_id#17, farm_id#18, secret#19, secret_original#20, extension_original#21, marker#22], [isnotnull(license#15)]
...
```
### Question 7
The following Spark command can be used to save the result on the disk using CSV file formal.
```java
interestingPicturesNDWithCache.write.
							   format("com.databricks.spark.csv").
                               option("header", "true").
                               save("result.csv")
```

## Processing data using RDDs
### Question 1

The following Spark command can be used to display the 5 lines of the RDD the number of elements in the RDD.
```java
// originalFlickrMeta: RDD[String]
originalFlickrMeta.take(5)
originalFlickrMeta.count()
```

### Question 2
The following Spark command can be used to transform RDD[String] in RDD[Picture] using Picture class.
```java
//originalFlickrMeta: RDD[String]
val pictures: RDD[Picture] = originalFlickrMeta.map(row => new Picture(row.split("\t")))
```
The following Spark command can be used to filter interesting pictures based on valid country and tags fields.
```java
val interstingPics: RDD[Picture] pictures.filter(pic => pic.hasValidCountry & pic.hasTags)
```
### Question 3
The following Spark command can be used to group images by country.
```java
val groupByCountry = interstingPics.groupBy(pic => pic.c)
// groupByCountry: [(Country, Iterable[Picture])]
```
### Question 4
The following Spark command can be used to process list so that first element is a country, and the second element is the list of tags used on pictures.
```java
val countriesTags = groupByCountry.map(x => (x._1, x._2.flatMap(pic => pic.userTags)))
// countriesTags: [(Country, Iterable[String])]
```
### Question 5
The following Spark command can be used to create mapping between tabs and number of their occurances.
```java
val tagsCount = countriesTags.map(x => (x._1, x._2.groupBy(identity).mapValues(_.size).map(identity)))
// tagsCount: [(Country, Map[String, Int])]
```
### Question 6
To not reduce the size of RDD from the very beginning and leverage the parallelization we can proceed in the following way. First we will do a map of all images to transform it to the following form: key = country, value = list of tuples of tags and the number 1 sorted in the lexicographical order.
```java
val intermRDD = interstingPics.map(x => (x.c, x.userTags.sorted.map(x => (x, 1))))
```
For example, if we have a picture from "Peru" with tags "Machu-Picchu" and "Andes", we will transform it to the following form: {key = "Peru", values = [("Andes", 1), ("Machu-Picchu", 1)]}. Thus, we will have as many values in the transformed RDD as the number of images in the database and tags are stored in a proper form to make a final reduce by key. Next we perform the reduceByKey function where for every couple of elements of RDD with the same key we sum frequences for repeating tags and concatenate with non-repeating keeping the lexicographical order.
```java
val finalRDD = interestingTags.reduceByKey((a, b) => sumOrConcatTags(a, b))
```
We can efficiently perform the last operation as we sorted the tags alphabetically beforehand. In the end we obtain the RDD of type RDD[(Country, Map[String, Int])] which is equivalent to the previous question. In contrast to the previous questions, we efficienlty used the parallelization even if tje number of countries is arbitrary small.





