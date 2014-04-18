Spark SQL, Avro and Parquet
===========================

This tutorial shows how to query data stored as Avro objects stored
inside a columnar format (Parquet) via the Spark SQL query
interface. The main intention of the tutorial is to show the seamless
integration of the functional RDD operators that come with Spark and
its SQL interface. For users who are unfamiliar with Avro we show how
to make use of Avro interface description language (IDL) inside a
Spark Maven project.

_Acknowledgments: Thanks to Matt Massie (@massie) and the ADAM
project for showing how to integrate Avro and Parquet with Spark._

Building the example
--------------------

```
$ git clone https://github.com/apache/spark.git
$ cd spark
$ sbt/sbt clean publish-local
```

Then in a different directory

```
$ git clone https://github.com/AndreSchumacher/avro-parquet-spark-example.git
$ cd avro-parquet-spark-example
$ mvn package
```

Project setup
-------------

Here we are using Maven to build the project due to the available Avro
IDL compiler plugin. Obviously one could have achieved the same goal
using sbt.

There are two subprojects:

* `example-format`, which contains the Avro description of the primary
  data record we are using (`User`)
* `example-code`, which contains the actual code that executes the
  queries

There are two ways to specify a schema for Avro records: via a
description in JSON format or via the IDL.  We chose the latter since
it is easier to comprehend.

Our example models the user database of a social network, where users
are asked to complete a personal profile which contains among other
things their name and favorite color. Users can also send text
messages to other users. The schemas of the resulting `User` and
`Message` records then look as follows.

```xml
@namespace("avrotest.avro")
protocol AvroSparkSQL {
    record User {
        // The name of the user
        string name = "unknown";
        // The age of the user
        int age = 0;
        // The favorite color of the user
        string favorite_color = "unknown";
    }
    record Message {
        // The ID of the message
        long ID = 0;
        // The sender of this message
        string sender = "unknown";
        // The recipient of this message
        string recipient = "unknown";
        // The content of the message
        string content = "";
    }
}
```

This file is stored as part of the `example-format` project and is
eventually compiled into a Java implementation of the class that
represents these two types of records. Note that the different
attributes are defined via their name, their type and an optional
default value. For more information on how to specify Avro records see
[the Avro documentation](http://avro.apache.org/docs/current/idl.html).

Part of the description is also the _namespace_ of the protocol, which
will result in the package name of the classes that will be generated
from the description. We use the Avro maven plugin to do this
transformation. It is added to `example-format/pom.xml` as follows:

```xml
<plugin>
    <groupId>org.apache.avro</groupId>
    <artifactId>avro-maven-plugin</artifactId>
</plugin>
```

Data generation
---------------

Once the code generation has completed, objects of type `User` can be
created via the `Builder` that was generated. For example:

```Scala
import avrotest.avro.User

User.newBuilder()
    .setName("User1")
    .setAge(10)
    .setFavoriteColor("blue")
    .build()
```

We can create a set of users and store these inside an Avro file as
follows.

```Scala
import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.SpecificDatumWriter
import avrotest.avro.User

val userDatumWriter = new SpecificDatumWriter[User](classOf[User])
val dataFileWriter = new DataFileWriter[User](userDatumWriter)
dataFileWriter.create(User.getClassSchema, file)

for(i <- 1 to 10) {
    dataFileWriter.append(createUser(i))
}

dataFileWriter.close()
```

Note that `createUser` in the above example is a factory method that
uses the `Builder` to create `User` objects as described above.
Similarly a set of messages can be created by using the class
`Message` instead of `User` and a corresponding factory method. It is
generally also possible to skip the step of code generation (for
example, when the schema is generated dynamically). In this case there
is similar but different approach using generic writers to write data
to Avro files.

Data stored in Avro format has the advantage of being accessible from
a large number of programming languages and frameworks for which there
exist Avro code generators. In order to process it via columnar
frameworks such as Parquet we need to convert the data first (or store
it in Parquet format right away). This tutorial assumes that you want
to convert Avro files to Parquet files stored inside, say, HDFS. The
conversion can be achieved as follows.

```Scala
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, IndexedRecord}
import org.apache.avro.mapred.FsInput
import parquet.avro.AvroParquetWriter
import avrotest.avro.User

val schema = User.getClassSchema
val fsInput = new FsInput(input, conf)
val reader =  new GenericDatumReader[IndexedRecord](schema)
val dataFileReader = DataFileReader.openReader(fsInput, reader)
val parquetWriter = new AvroParquetWriter[IndexedRecord](output, schema)

while(dataFileReader.hasNext)  {
    parquetWriter.write(dataFileReader.next())
}

dataFileReader.close()
parquetWriter.close()
```

Here `input` is the (Hadoop) path under which the Avro file is stored,
and `output` is the destination (Hadoop) path for the result Parquet
file. Note that different from above we are using the generic variant
of the Avro readers. That means that we only changing the `schema = ...`
line we can actually convert any Avro file adhering to that schema
to a corresponding Parquet file.

Import into Spark SQL
---------------------

The data written in the last step can be directly imported as a table
inside Spark SQL and then queried. This can be done as follows.

```Scala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

val conf = new SparkConf(true)
    .setMaster("local")
    .setAppName("ParquetAvroExample")
val sqc = new SQLContext(new SparkContext(conf))

val schemaUserRdd = sqc.parquetFile(parquetUserFile.getParent.toString)
schemaUserRdd.registerAsTable("UserTable")

val schemaMessageRdd = sqc.parquetFile(parquetMessageFile.getParent.toString)
schemaMessageRdd.registerAsTable("MessageTable")
```

Here `parquetUserFile` is the path under which the previously
generated Parquet file containing the user data was stored. It is
important to note that we do not need to specify a schema when we
import the data, since the schema is preserved all the way from the
initial specification in Avro IDL to the registration as a table
inside Spark SQL.

Querying the user and message databases
---------------------------------------

After the tables have been registered, they can queried via SQL
syntax, for example:

```Scala
sqc.sql("SELECT favorite_color FROM UserTable WHERE name = \"User5\"")
    .collect()
```

The result will be returned as a sequence of `Row` objects, whose
fields can be accessed via `apply()` functions. Also more complicated
operations can be performed, for example:

```Scala
sql("""SELECT name, COUNT(recipient) FROM
       UserTable JOIN MessageTable ON UserTable.name = MessageTable.sender
       GROUP BY name ORDER BY name""")
    .collect()
```

The last example will generate a list of pairs of usernames and
counts, correponding to the number of messages that user has sent.

Mixing SQL and other Spark operations
-------------------------------------

Since SQL data is stored as RDDs that have a schema attached to them
(hence, `SchemaRDD`), SQL and other operations on RDDs can be mixed
freely, for example:

```Scala
sqc.sql("SELECT content from MessageTable")
    .flatMap(row => row.getString(0).replace(",", "").split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)
    .collect()
    .toMap
```

The previous example counts the number of times each word appears in
any of the messages in the MessageTable.

Importing Avro objects directly as RDD
--------------------------------------

It is also possible to make direct use of the code-generated Avro
classes in Spark. This requires registering a special Kryo serializer
for each of the generated classes. Look at the example code for how
this is done. The data can then be directly manupulated via Spark's
Scala API. For example:

```Scala
def myMapFunc(user: User): String = user.toString

val userRDD: RDD[User] = readParquetRDD[User](sc, parquetFileName)
userRDD.map(myMapFunc).collect().foreach(println(_))
```
