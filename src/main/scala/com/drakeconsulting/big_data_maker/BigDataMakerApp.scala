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
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

object SimpleApp {
  def main(args: Array[String]) {

    case class Config(outDir: String = ".", numPartitions: Integer = 0, numRows: Integer = 0)

    val parser = new scopt.OptionParser[Config]("Big Data Maker") {
      head("Big Data Maker", "1.0")
      opt[String]('o', "outDir").required.action( (x,c) => c.copy(outDir = x) ).text("outDir is a directory")
      opt[Int]('p', "numPartitions").action( (x,c) => c.copy(numPartitions = x) ).text("# of partitions")
      opt[Int]('r', "numRows").action( (x,c) => c.copy(numRows = x) ).text("# of rows/partition")
    }

    var outDir = ""
    var numPartitions: Integer = 0
    var numRows: Integer = 0

    parser.parse(args, Config()) match {
      case Some(config) =>
        outDir = config.outDir
        numPartitions = config.numPartitions
        numRows = config.numRows

      case None =>
        println("Oops.")
        System.exit(1)
    }

    if (numPartitions == 0) {
      numPartitions = 10
    }
    if (numRows == 0) {
      numRows = 1000
    }

    println("outDir=" + outDir)
    println("numPartitions=" + numPartitions)
    println("numRows=" + numRows)

    val conf = new SparkConf().setAppName("Big Data Maker Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val states = "AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MT NE NV NH NJ NM NY NC ND OH OK OR MD MA MI MN MS MO PA RI SC SD TN TX UT VT VA WA WV WI WY".split(" ").toList
    val bigData = new BigData(sqlContext, outDir, numPartitions, numRows)
    bigData.addColumn(new StringConstant("f1", "testing"))
    bigData.addColumn(new RandomLong("f2", 100000000000L))
    bigData.addColumn(new RandomLong("f3", 10000000000L))
    bigData.addColumn(new RandomLong("f4", 1000000000L))
    bigData.addColumn(new RandomDouble("f5", 1000000000.0))
    bigData.addColumn(new RandomDouble("f6", 100000000.0))
    bigData.addColumn(new RandomDouble("f7", 10000000.0))
    bigData.addColumn(new Categorical("states", states))

    bigData.writeFile

  }
}
