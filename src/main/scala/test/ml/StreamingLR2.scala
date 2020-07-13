package test.ml

import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingLR2 {
  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext("local[3]", "streaming lr", Seconds(5))
    val stream = ssc.socketTextStream("localhost", 7789)

    val numFeatures = 10
    // 零向量作为初始化权重
    val zeroVector = DenseVector.zeros[Double](numFeatures)
    println(zeroVector.data.toList)
    val model1 = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.dense(zeroVector.data))
      .setNumIterations(1) //迭代次数
      .setStepSize(0.1) //步长

    val model2 = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.dense(zeroVector.data))
      .setNumIterations(1) //迭代次数
      .setStepSize(1) //步长

    val labeledStream = stream.map{event =>
      val split = event.split("\t")
      val y = split(0).toDouble
      val features = split(1).split(",").map(_.toDouble)
      LabeledPoint(label = y, features = Vectors.dense(features))
    }

    // 在同一个流上训练两个模型
    model1.trainOn(labeledStream)
    model2.trainOn(labeledStream)

    // 计算误差
    val errors = labeledStream.transform{rdd =>
      val latest1 = model1.latestModel()
      val latest2 = model2.latestModel()
      rdd .map{point =>
        val predict1 = latest1.predict(point.features)
        val predict2 = latest2.predict(point.features)
        (predict1 - point.label, predict2 - point.label)
      }
    }

    errors.foreachRDD{(rdd, time) =>
      val mse1 = rdd.map{case(err1, err2) => err1*err1}.mean()
      val rmse1 = math.sqrt(mse1)
      val mse2 = rdd.map{case(err1, err2) => err2*err2}.mean()
      val rmse2 = math.sqrt(mse2)
      println(
        s"""
          |====================================================
          |时间: $time
          |====================================================
          |""".stripMargin)
      println(s"MSE: $mse1 , $mse2")
      println(s"RMSE: $rmse1 , $rmse2")

    }
    errors.foreachRDD{rdd => println("1")}


    ssc.start()
    ssc.awaitTermination()
  }
}
