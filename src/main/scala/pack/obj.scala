package pack
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

object obj {
  case class schema(
                   txno:String,
                   category:String,
                   product:String,
                   spendby:String
                   )
  def main(args:Array[String]):Unit={
  val conf = new SparkConf().setAppName("first").setMaster("local[*]")
  val sc  = new SparkContext(conf)

  sc.setLogLevel("ERROR")
//  sc.setLogLevel("INFO")
  val spark = SparkSession.builder.getOrCreate()
  import spark.implicits._
  val csvdf = spark.read.format("csv").option("header","true").load("file:///Users/vasudevareddynemali/Documents/Spark_files/df.csv")
//  val filtdf  = csvdf.filter(!(col("category") isin("Exercise","Team Sports")))
  csvdf.show()
  val reldf = csvdf.selectExpr("cast(id as string) as sid",
                                "split(tdate,'-')[2] as year",
                                "month(from_unixtime(unix_timestamp(tdate,'MM-dd-yyyy')))",
                                "amount+100 as amount",
                                "upper(category)",
                                "coalesce(product,'NA')",
                                "concat(spendby,' zeyo')",
                                "case when spendby='cash' then 0 else 1 end as status"
                                )
  reldf.printSchema()
  val upperdf = csvdf.withColumn("category",expr("upper(category)"))
                      .withColumn("amount",expr("amount+100"))
                      .withColumn("tdate",expr("split(tdate,'-')[2]"))
                      .withColumn("product",expr("coalesce(product,'zeyo')"))
                      .withColumn("status",expr("case when spendby='cash' then 0 else 1 end" ))
                      .withColumn("spendby",expr("concat(spendby,' zeyo ')"))

//  upperdf.show()
//  reldf.show()

  val df = Seq(
              (1,"raj"),
              (2,"ravi"),
              (3,"sai"),
              (5,"rani")
              ).toDF("id","home")
  df.show()
  val df1 = Seq(
    (1,"mouse"),
    (3,"mobile"),
    (7,"laptop")
  ).toDF("id","product")

  df1.show()

  val join_inner = df.join(df1,Seq("id"),"inner")
//  join_inner.show()

  val join_left = df.join(df1,Seq("id"),"left")
//  join_left.show()
//
//  val join_right = df.join(df1,Seq("id"),"right")
//  join_right.show()
  val join_full = df.join(df1,Seq("id"),"full")
  join_full.show()
  csvdf.show()
  csvdf.withColumn("tdate1",expr("year(from_unixtime(unix_timestamp(tdate,'MM-dd-yyyy')))")).show()
  }
}
