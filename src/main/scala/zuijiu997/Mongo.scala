package zuijiu997

import com.mongodb.spark.MongoSpark

case class T(t1:Int, t2:Int)

object Mongo {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder()
//      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://root:zy79117911#@dds-wz9fcbcc474f93e41372-pub.mongodb.rds.aliyuncs.com:3717,dds-wz9fcbcc474f93e42901-pub.mongodb.rds.aliyuncs.com:3717/admin")
      .config("spark.mongodb.input.database", "TouTiao")
      .config("spark.mongodb.input.collection", "toutiaoIncrement")
      .config("spark.mongodb.output.uri", "mongodb://admin:zy79117911#@dds-wz9fcbcc474f93e41372-pub.mongodb.rds.aliyuncs.com:3717,dds-wz9fcbcc474f93e42901-pub.mongodb.rds.aliyuncs.com:3717/admin")
      .config("spark.mongodb.output.database", "TouTiao")
      .config("spark.mongodb.output.collection", "testspark")
//      .config("spark.mongodb.input.partitionerOptions.partitionKey", "_id")
      .config("spark.mongodb.input.partitionerOptions.partitionSizeMB", 10)
      .getOrCreate()

    val rdd = MongoSpark.load(spark.sparkContext)
    val documents = rdd.take(10)
    for (elem <- documents) {
      println(elem.toJson)
    }

  }
}
