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

class BigData(val sqlContext:SQLContext, val filename: String, val numPartitions:Integer, val numRowsPerPartition:Integer) extends Serializable {
    var cols = Seq[AbstractCol]()
    var df: org.apache.spark.sql.DataFrame = _
    val distData = sqlContext.sparkContext.parallelize(Seq[Int](), numPartitions)
    
    def addColumn(col: AbstractCol) {
        cols = cols :+ col
    }
    
    def createRow(index: Integer): org.apache.spark.sql.Row = {
        org.apache.spark.sql.Row.fromSeq(cols.map(x => {
            x.getValue(index)
        }))
    }

    def _createSchema(): org.apache.spark.sql.types.StructType = {
        org.apache.spark.sql.types.StructType(cols.map(x => {
            org.apache.spark.sql.types.StructField(x.name, x.dataType)
        }))
    }
    
    def _createRDD() = {
        distData.mapPartitionsWithIndex((index, iter) => {
            (1 to numRowsPerPartition).map(_ => {
                createRow(index)
            }).iterator
        })
    }
    
    def _createDataFrame(): org.apache.spark.sql.DataFrame = {
        df = sqlContext.createDataFrame(_createRDD, _createSchema)
        df
    }
    
    def writeFile() {
        _createDataFrame()
        df.write.mode("overwrite").parquet(filename)
    }
}
