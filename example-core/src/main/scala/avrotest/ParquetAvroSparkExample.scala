/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package avrotest

import com.google.common.io.Files

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.util.getTempFilePath

import avrotest.UserOperations._
import avrotest.avro.User
import avrotest.avro.Message

object ParquetAvroSparkExample {

  private var sqc: SQLContext = _

  def main(args: Array[String]) {
    // Create a Spark Configuration for local cluster mode
    val conf = new SparkConf(true)
      .setMaster("local")
      .setAppName("ParquetAvroExample")

    // Register Kryo serializers
    // Note: this is only required if we would like to use User objects
    // inside Spark's MapReduce operations. For Spark SQL this is not
    // required.
    setKryoProperties()

    // Create a Spark Context and wrap it inside a SQLContext
    sqc = new SQLContext(new SparkContext(conf))

    // Prepare some input data
    val avroFiles = (getTempFilePath("users", ".avro"), getTempFilePath("messages", ".avro"))
    val parquetFiles = (
      new Path(Files.createTempDir().toString, "users.parquet"),
      new Path(Files.createTempDir().toString, "messages.parquet"))

    // Generate some input (100 users, 1000 messages) and write then as Avro files to the local
    // file system
    writeAvroFile(avroFiles._1, createUser, 100)
    writeAvroFile(avroFiles._2, createMessage(100)_, 1000)
    // Now convert the Avro file to a Parquet file (we could have generated one right away)
    convertAvroToParquetAvroFile(
      new Path(avroFiles._1.toString),
      new Path(parquetFiles._1.toString),
      User.getClassSchema,
      sqc.sparkContext.hadoopConfiguration)
    convertAvroToParquetAvroFile(
      new Path(avroFiles._2.toString),
      new Path(parquetFiles._2.toString),
      Message.getClassSchema,
      sqc.sparkContext.hadoopConfiguration)

    // Import the Parquet files we just generated and register them as tables
    sqc.parquetFile(parquetFiles._1.getParent.toString)
      .registerAsTable("UserTable")
    sqc.parquetFile(parquetFiles._2.getParent.toString)
      .registerAsTable("MessageTable")

    // Now let's do some queries
    println("The age of User3:")
    println(findAgeOfUser("User3", sqc))
    println("The favorite color of User4:")
    println(findFavoriteColorOfUser("User4", sqc))
    println("Favorite color distribution:")
    val result = findColorDistribution(sqc)
    for (color <- result.keys) {
      println(s"color: $color count: ${result.apply(color)}")
    }
    findNumberOfMessagesSent(sqc).foreach {
      case (sender, messages) => println(s"$sender sent $messages messages")
    }
    findMutualMessageExchanges(sqc).foreach {
      case (user_a, user_b) => println(s"$user_a and $user_b mutually exchanged messages")
    }
    println("Count words in messages:")
    countWordsInMessages(sqc).toTraversable.foreach {
      case (word, count) => println(s"word: $word count: $count")
    }
  }

  /**
   * Note1: Spark uses Kryo for serializing and deserializing Objects contained in RDD's
   * and processed by its functional operators. In order to use Avro objects as part
   * of those operations we need to register them and specify and appropriate (De)Serializer.
   * Note2: This step is not neccesary if we one rely on relation operations of Spark SQL,
   * since these use Row objects that are always serializable.
   */
  def setKryoProperties() {
    System.setProperty("spark.kryo.registrator", "avrotest.SparkAvroKryoRegistrator")
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.kryoserializer.buffer.mb", 4.toString)
    System.setProperty("spark.kryo.referenceTracking", "false")
  }
}
