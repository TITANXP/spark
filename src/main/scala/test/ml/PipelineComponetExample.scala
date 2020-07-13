package test.ml

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.sql
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.linalg.Vector

/**
 * pipeline示例
 */
object PipelineComponetExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("pipeline").master("local[3]").getOrCreate()

    // 训练数据
    val training = spark.createDataFrame(Seq(
      (0L, "a b c spark", 1.0),
      (1L, "c d", 0.0),
      (2L, "spark f g", 1.0),
      (3L, "spark mr", 0.0)
    )).toDF("id", "text", "label")

    // 配置一个pipeline
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")

    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.01)

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))

    val model = pipeline.fit(training)

    // 可以将拟合好的pipeline保存到磁盘
    model.write.overwrite().save("temp/pipeline_example/model")
    // w未拟合的也可以保存到磁盘
    pipeline.write.overwrite().save("temp/pipeline_example/pipline")

    //载入模型
    val sameModel = PipelineModel.load("temp/pipeline_example/model")

    //测试数据
    val test = spark.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "spark hadoop spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    model.transform(test)
      .select("id", "text", "probability", "prediction")
      .collect()
      .foreach{case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
          println(s"($id, $text) --> prob=$prob, prediction=$prediction")
      }



  }
}
