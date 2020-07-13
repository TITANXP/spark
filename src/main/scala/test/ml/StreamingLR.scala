package test.ml

import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingLR {
  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext("local[3]", "streaming lr", Seconds(5))
    val stream = ssc.socketTextStream("localhost", 7789)

    val numFeatures = 10
    // 零向量作为初始化权重
    val zeroVector = DenseVector.zeros[Double](numFeatures)
    println(zeroVector.data.toList)
    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.dense(zeroVector.data))
      .setNumIterations(1) //迭代次数
      .setStepSize(0.1) //步长

    val labeledStream = stream.map{event =>
      val split = event.split("\t")
      val y = split(0).toDouble
      val features = split(1).split(",").map(_.toDouble)
      LabeledPoint(label = y, features = Vectors.dense(features))
    }

    model.trainOn(labeledStream)
    model.predictOn(labeledStream.map(x => x.features)).print()
//    labeledStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
