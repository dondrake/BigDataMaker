package com.drakeconsulting.big_data_maker

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructField, StringType, LongType, DoubleType}

class ColumnsTest extends FunSuite with SharedSparkContext {
  val numLoops = 100

  test("test StringConstant") {
    val s1 = new StringConstant("f1", "abc")
    assert("abc" === s1.getValue(1))
    assert(StructField("f1", StringType, false) == s1.getStructField)
  }

  test("test RandomLong") {
    val s1 = new RandomLong("f1", 666666L)
    for (x <- 1 to numLoops) {
      assert(s1.getValue(1) >= 0)
      assert(s1.getValue(1) <= 666666L)
    }
    assert(StructField("f1", LongType, false) == s1.getStructField)
  }

  test("test RandomDouble") {
    val s1 = new RandomDouble("f1", 666666.00)
    for (x <- 1 to numLoops) {
      assert(s1.getValue(1) >= 0)
      assert(s1.getValue(1) <= 666666.00)
    }
    assert(StructField("f1", DoubleType, false) == s1.getStructField)
  }

  test("test Categorical") {
    val list = List("a", "b", "c", "d")
    val s1 = new Categorical("f1", list)
    for (x <- 1 to numLoops) {
      val v = s1.getValue(1)
      assert(list.exists(key => v.contains(key)))
    }
    assert(StructField("f1", StringType, false) == s1.getStructField)
  }
}
  
