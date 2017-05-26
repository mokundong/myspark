package com.mkd.mllib

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
/**
  * Created by yousa on 2017/5/26.
  */
object testVector {
  def main(args: Array[String]): Unit = {
    //建立密集向量
    val vd: Vector = Vectors.dense(2,0,6)
    //打印密集向量第三个值
    println(vd(2))
    //建立稀疏向量
    val vs: Vector = Vectors.sparse(4,Array(0,1,2,3),Array(9,5,2,7))
    //打印稀疏向量第三个值
    println(vs(2))
  }
}
