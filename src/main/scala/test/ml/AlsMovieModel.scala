package test.ml

import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{Dataset, SparkSession}

object AlsMovieModel {
  case class Rating(userId: Int, movieId: Int, rating: Float)

  def parseRating(line: String): Rating = {
    val fields = line.split("\t")
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat)
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[4]").appName("als_model").getOrCreate()

    import spark.implicits._ //隐式转换
    val ratings = spark.read.textFile("D:\\AXION\\下载\\dataSource\\ml-100k\\u.data")
      .map(parseRating)
    println(ratings.take(10).toList)

    //分割测试集和训练集
    val Array(train, test) = ratings.randomSplit(Array(0.8, 0.2))
    println(train.count() + ";" + test.count())

    //训练模型
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.1)
      .setRank(3)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
      .setImplicitPrefs(false) // 是否使用隐式反馈
    val model = als.fit(train)

    // 用户因子矩阵
    println(model.userFactors.collect().toList)
    // 物品因子矩阵
    println(model.itemFactors.collect().toList)

    val predict = model.transform(test)
    println(predict.collect().toList)

    // 为每个用户推荐2个物品
    val recommend = model.recommendForAllUsers(2)
    println(recommend.collect().array)

    val movies = spark.read.textFile("D:\\AXION\\下载\\dataSource\\ml-100k\\u.item")
    val titles = movies.map(line => line.split("\\|").take(2))
      .map(array => (array(0).toInt, array(1)))
      .collect()
    println(titles)
  }
}
