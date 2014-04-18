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

import scala.util.Random

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterEach

import com.google.common.io.Files

import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.util._

import avrotest.UserOperations._
import avrotest.avro.{Message, User}

// inspired by [[org.apache.spark.LocalSparkContext]]
class UserTestSuite extends FunSuite with BeforeAndAfterEach {

  @transient var sqc: SQLContext = _

  override def beforeEach() {
    println("creating spark context")
    val conf =
      new SparkConf(false)
        .setMaster("local")
        .setAppName("test")
    sqc = new SQLContext(new SparkContext(conf))

    UserOperations.random = new Random(15485863l)

    val avroFiles = (getTempFilePath("users", ".avro"), getTempFilePath("messages", ".avro"))
    val parquetFiles = (
      new Path(Files.createTempDir().toString, "users.parquet"),
      new Path(Files.createTempDir().toString, "messages.parquet"))

    // Generate some input (100 users, 1000 messages) and write it as an Avro file to the local
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
  }

  override def afterEach() {
    resetSparkContext()
  }

  private def resetSparkContext() = {
    if (sqc != null) {
      println("stopping Spark Context")
      sqc.sparkContext.stop()
    }
    sqc = null
  }

  test("Favorite color") {
    assert(findFavoriteColorOfUser("User1", sqc) === "red")
  }

  test("Age") {
    assert(findAgeOfUser("User20", sqc) === 2)
  }

  test("Color distribution") {
    assert(findColorDistribution(sqc).apply("red") === 4)
  }

  // Note: this test could fail because of a different random generator implementation
  // although the seed is fixed
  test("Number of messages") {
    val tmp = findNumberOfMessagesSent(sqc)
    assert(tmp(0)._1 === "User1")
    assert(tmp(0)._2 === 12)
  }

  // Note: this test could fail because of a different random generator implementation
  // although the seed is fixed
  test("Mutual message exchange") {
    val finder = findMutualMessageExchanges(sqc)
      .find {
        case (user1: String, user2: String) => user1 == "User0" && user2 == "User43"
      }
    assert(finder.isDefined)
  }

  test("Count words in messages") {
    val tmp = countWordsInMessages(sqc)
    assert(tmp("Hey") === 1000)
  }
}
