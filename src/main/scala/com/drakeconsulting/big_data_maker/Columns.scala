/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package com.drakeconsulting.big_data_maker

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataType, StructField, StringType, LongType, DoubleType}
import java.util.Random

abstract class AbstractCol(val name: String, val dataType:DataType, val nullable:Boolean = true) extends Serializable {
  val random = new Random(123)
  def getStructField(): StructField = {
    new StructField(name, dataType, nullable)
  }
  def getValue(index: Integer):Any

}

class StringConstant(name: String, val constant:String) extends AbstractCol(name, StringType, nullable=false) {
  override def getValue(index: Integer): String = {
    constant
  }
}

class RandomLong(name: String, val maxValue:Long) extends AbstractCol(name, LongType, nullable=false) {
  override def getValue(index: Integer): Long = {
    (random.nextDouble * maxValue).toLong
  }
}

class RandomDouble(name: String, val maxValue:Double) extends AbstractCol(name, DoubleType, nullable=false) {
  override def getValue(index: Integer): Double = {
    random.nextDouble * maxValue
  }
}

