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

import java.io.File

import org.apache.hadoop.fs.Path

import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.mapred.FsInput
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}

import parquet.avro.AvroParquetWriter

import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.expressions.Row

// our own class generated from user.avdl by Avro tools
import avrotest.avro.User

// Implicits
import collection.JavaConversions._

object UserOperations {

  /**
   * For the given username, find the favorite number of the user.
   *
   * @param name User name
   * @param sqc The SQLContext to use
   * @return The favorite number
   */
  def findFavoriteNumberOfUser(name: String, sqc: SQLContext): Int = {
    sqc.sql("SELECT favorite_number FROM UserTable WHERE name = \"" + name + "\"")
      .collect()
      .apply(0)
      .apply(0)
      .asInstanceOf[Int]
  }

  /**
   * For the given username, find the favorite color of the user.
   *
   * @param name User name
   * @param sqc The SQLContext to use
   * @return The favorite color
   */
  def findFavoriteColorOfUser(name: String, sqc: SQLContext): String = {
    sqc.sql("SELECT favorite_color FROM UserTable WHERE name = \"" + name + "\"")
      .collect()
      .apply(0)
      .apply(0)
      .asInstanceOf[String]
  }

  /**
   * For every color that is contained in the table, find the number of
   * users whose favorite color matches that color.
   *
   * @param sqc The SQLContext to use
   * @return A map of colors to the number of its occurrences
   */
  def findColorDistribution(sqc: SQLContext): Map[String, Int] = {
    val result = new collection.mutable.HashMap[String, Int]()
    val colorCounts = sqc.sql("SELECT favorite_color, COUNT(name) FROM UserTable GROUP BY favorite_color")
      .collect()
    for(row <- colorCounts) {
      result += row(0).asInstanceOf[String] -> row(1).asInstanceOf[Int]
    }
    result.toMap
  }

  /**
   * For every user find the user(s) that have that particular user at the head
   * of their friends list.
   *
   * @param sqc The SQLContext to use
   * @return A list of pairs `(a, b)`, which means that `b` has `a` as her/his best
   *         friend
   */
  def findInverseBestFriend(sqc: SQLContext): Seq[(String, String)] = {
    sqc.sql("SELECT name, friends FROM UserTable")
      .registerAsTable("UserFriends")
    sqc.sql("SELECT UserTable.name, UserFriends.name FROM UserTable JOIN UserFriends ON UserTable.name = UserFriends.friends[0]")
      .collect()
      .map(row => (row(0).asInstanceOf[String], row(1).asInstanceOf[String]))
  }

  /**
   * For every user count the number of times it appears on some other
   * user's friend list.
   *
   * @param sqc The SQLContext to use
   * @return A list of (userName, count) pairs
   */
  def findNumberOfFriendshipCircles(sqc: SQLContext): Seq[(String, Int)] = {
    sqc.sql("SELECT friends FROM UserTable")
      .flatMap(row => row(0).asInstanceOf[Row].seq)
      .groupBy(_.asInstanceOf[String])
      .map(pair => (pair._1, pair._2.size))
      .sortByKey()
      .collect()
  }

  /**
   * Generates a set of users and writes their Avro objects to an Avro file.
   *
   * @param file The output file
   * @param numberOfUsers The number of users to generate
   */
  def writeAvroUsersFile(file: File, numberOfUsers: Int): Unit = {
    val userDatumWriter = new SpecificDatumWriter[User](classOf[User])
    val dataFileWriter = new DataFileWriter[User](userDatumWriter)
    dataFileWriter.create(User.getClassSchema, file)

    for(i <- 1 to numberOfUsers) {
      dataFileWriter.append(createUser(i))
    }

    dataFileWriter.close()
  }

  /**
   * Converts an Avro file that contains a set of users to a Parquet file.
   *
   * @param input The Avro file to convert
   * @param output The output file (possibly on HDFS)
   * @param conf A Hadoop `Configuration` to use
   */
  def convertAvroToParquetAvroUserFile(input: Path, output: Path, conf: Configuration): Unit = {
    val fsInput = new FsInput(input, conf)
    val reader =  new SpecificDatumReader[User](classOf[User])
    val dataFileReader = DataFileReader.openReader(fsInput, reader)
    val parquetWriter = new AvroParquetWriter[User](output, User.getClassSchema)

    while(dataFileReader.hasNext)  {
      // Mote: the writer does not copy the passed object and buffers
      // writers. Therefore we need to pass a new User object every time,
      // although the reader allows us to re-use the same object.
      parquetWriter.write(dataFileReader.next())
    }

    dataFileReader.close()
    parquetWriter.close()
  }

  /**
   * Creates a user Avro object.
   *
   * @param id The ID of the user to generate
   * @return An Avro object that represents the user
   */
  def createUser(id: Int): User = {
    val builder = User.newBuilder()
      .setName(s"User$id")
      .setFavoriteNumber(id)
      .setFriends(List(s"User${id+1}", s"User${id+2}", s"User${id+3}"))
    if (id >= 5) {
      builder
        .setFavoriteColor("blue")
        .build()
    } else {
      builder
        .setFavoriteColor("red")
        .build()
    }
  }
}
