# Chapter 1: Introduction to Flink
# Running flink in command line
>> Download https://archive.apache.org/dist/flink/flink-1.8.0/ 
tar -xvzf flink-1.8.0-bin-scala_2.11.tgz
./bin/start-cluster.sh
>> navigate to localhost:8081
# Optional: run flink on different port
vi flink-conf.yaml
>> uncomment port and change to 9081
>> you can run multiple cluster by copying the flink folder in diffrent >> directory and running a new cluster on different port

# Start scala shell
>> Helps in writting flink code iteractively
./bin/start-scala-shell.sh local
>> You can also connect to remote cluster
./bin/start-scala-shell.sh remote <host> <port>

# Launch flink in ec2 machine
>> create ec2 machine
>> ssh into machine
ssh -i flink.pem ec2-user@<public-ip-address>
>> Download flink on ec2
wget http://apache.mirrors.tds.net/flink/flink-1.8.0/flink-1.8.0-bin-scala_2.11.tgz
tar -xvzf flink-1.8.0-bin-scala_2.11.tgz
>> install java
sudo yum install java
>> Try starting / stopping cluster
./bin/start-cluster.sh
./bin/stop-cluster.sh
>> Start local shell
./bin/start-scala-shell.sh local
>> Navigate to UI
<ec2-public-ip-address>:8081

# Chapter 2: Using a flink cluster UI and Data Onboarding
# Simple flink app. Read data (lazily) and count rows
scala> val dataSet = benv.readTextFile("/work/data/OnlineRetail.csv")
scala> dataSet.count()
>> Notice UI after running above commands and see datasource and datasink created for above simple job

# Flink programming model
>> Distributed data collections are initially created from sources (for example, by reading from files, kafka topics, or local or in-memory collections)
>> Computational logic that implements transformations and aggregations on distributed collections (for example, filtering, mapping, updating state, joining, grouping, defining windows, and aggregating)
>> Results are returned via sinks, which may, for example, write the data to (distributed) files, or to standard output (for example, the command-line terminal)

# levels of abstraction
>> 4. low level building block (streams, state, [event] time) : Stateful stream porcessing
>> 3. Core Apis : Datastream / Dataset API
>> 2. Declarative DSL: Table API
>> 1. Hight level language: SQL 

>> The lowest level of abstraction simply offers stateful streaming
>> Most applications would program against core APIs such as DataStream API (bounded/unbounded streams) eg. datastore, kafka, tweets ec. and the DataSet API (bounded datasets)
>> The table API is a declarative Domain Specific language (DSL) centered around tables, which may be dynamically changing tables
>> The highest level abstraction offered by flink is SQL, which makes is feasible to simply write SQL instead of API-driven code

# Batch processing
>> Typically used to process massive amounts of data collected in data lakes/warehouses
>> Data does not change at all or at least not much
>> SLAs are not too uptight in terms of how much time we can take to process a given set of data

# What is dataset?
>> Datasets in flink are bounded steams primarily used to batch processing jobs
>> Flink doesn't differentiate between data streams and datasets
>> Datasets are immutable collections of data that can contain duplicates
>>>> Mutable means changeable
>>>> Immutable means not changeable
>> Multiple operations and processes can be performed on immutable data

# Data types
>> Java tuples and scala case classes, primitive types, regular classes, hadoop writable, java pojos, values, special types 
>> Distribued data systems partition the data in a file in multiple blocks (eg. S3) say 4 each of size 64 kb. so total data in file is 64 * 4 kb. Its easy to recover blocks than entire file.
>> applicaton is submitted to jobmanager which talks to resource manger about how many resouces it needs. Resource mangager provides the task managers to the job manager which then talks to task managers (JVM process running on different machines) ((which have multiple task slots) directly to allocate apporpiate compute resources which are cpu and memory. 

# Operations
val text = env.fromElements("hello", "how are you", "thank you")
val counts = text.flatMap{_.toLowerCase.split("\\W+") filter {_.nonEmpty}}.map{(_,1)}.groupBy(0).sum(1).counts.print()

# Transformations
>> Data transformations transform one or more datasets into a new dataset
>> Programs can combine multiple transformations into sophisticated assemblies 
data.map{x => x.toInt}
data.flatMap {str => str.split(" ")}
data.filter{_ > 1000}

# Writing Data
>> Data sinks consume datasets and are used to store or return them 
textData.writeAsText("file:///my/result/on/localFs")
textData.writeAsText("hdfs://nnHost:nnPort/my/result/on/localFs")
values.writeAsCsv("file:///path/to/the/result/file", "\n", "|")

# Stream Processing
>> Data is changing all the time
>> Stream processing is typically used to handle internet of things (IoT) use cases with real-time processing
>> SLAs are rigid as real-time data typically must be processed on a real-time basis, to serve some other process

# What is Data Stream?
>> Data streams in Flink are bounded/unbounded streams primarily used for stream processing jobs
>> Transformations can be done in real time
>> Preferable for large operations
>> Data streams are immutable collections of data that can contain duplicates
>> Data streams support many types of transformations, aggregations, and other computations
>> Data streams also support saving or persistence into many types of sinks(output)

# Data types
>> Java tuples and scala case classes, java POJOs, primitive types, regular classes, values, hadoop writable, special types.

# Chapter 3: Batch Analytics with Apache Flink - Transformations
# Reading Data
>> Data sources create the initial datasets, such as from files or from java collections
val localLines = env.readTextFile("file::///path/to/my/textfile")
val hdfsLines = env.readTextFile("hdfs://hadoop/path/to/my/textfile")
val csvInput = env.readCsvFile[(Int, String, Double)]("hdfs:///the/CSV/file")

# Operations
val text = env.socketTextStream("localhost", 9999)
>> Perform operations in real time
val counts = text.flatMap{_.toLowerCase.split("\\W+") filter {_.nonEmpty}}.map{(_,1)}.keyBy(0).timeWindow(Time.seconds(5)).sum(1)
counts.print()

# Transformations
>> Data transformations transform one or more data streams into a new data stream
>> Programs can combine multiple transformations into sophisticated assemblies
data.map { x => x.toInt }
data.flatMap { str => str.split(" ") }
data.filter { _ > 1000 }
>> Window operations:
>>> Works as a continuous stream
>>> For example, the windowed view of a train compartment

# Writing Data
textData.writeAsText("file:///my/result/on/localFS")
textData.writeAsText("hdfs://nnHost:nnPort/my/result/on/localFS")
values.writeAsCsv("file:///path/to/the/result/file", "\n", "|")

# Batch Analytics
>> Loading a file into a Dataset
val dataSet = benv.readTextFile("/work/data/OnlineRetail.csv")
dataSet.count()
dataSet.first(5).print()
val array  = benv.fromElements("Foo", "bar", "foobar", "fubar")
array.count()
array.first(5).print()

# Basic Operations on a Dataset
>> Split the record row into columns:
val splitData = dataSet.map(line => (line.split(",")(2).trim(), line.split(",")(3).trim()))
splitData.first(5).print()

# Saving the Dataset
splitData.writeAsCsv("file:///work/data/out1.csv")
benv.execute()

# Transformations
>> Count the number of rows in a dataset
val dataset = benv.readTextFile("/work/data/OnlineRetail.csv")
dataSet.count()
dataSet.first(5).print()
val noHeader = dataSet.filter(!_.startWith("InvoiceNo"))
## Map transforms one dataset into another
noHeader.first(5).print()
### splits the data into a array i.e. each row is converted into an array
val splitData = noHeader.map(line => line.split(",")(2))
 ## Flatmap takes a collection as data set.  flattens the arrays further into tokens
 val splitData = noHeader.flatMap(line => line.split(","))
 splitData.first(5).print()

## distinct
val goodrecords = noHeader.filter(line => line.split(",").length == 8)
goodrecords.count()
val splitData = goodrecords.map(line => (line.split(",")(2).trim(), line.split(",")(3).trim().toInt()))
splitData.first(5).print()
splitData.distinct().count()

## minBy
### minBy second column
splitData.minBy(1).print()
splitData.maxBy(1).print()

## rebalance
>> Evenly rebalances the parallel partitions of a dataset to eliminate data skew
>> Only map-like transformations may follow a rebalance transformation

## union
val errors = noHeader.filter(line => line.split(",").length != 8)
errors.count()
val full = noHeader.count()
val both = goodrecords.union(errors)
both(count) // equals to full

## Partitioning
>> Partitions data into multiple devices to improve performance
>> Four different types of partitions:
>>> Hash partition: Takes a specific entity in a list of fields. Partitions using hashing. Can be retrieved from different devices with a similar hash. Hash partitions on a given key. Keys can be specified as position keys, expression keys, or key selector functions
val repar = splitData.partitionByHash(0).mapPartition{in => in.map{(_,1)}}
>>> Range partition: Used to determine the number of rows and columns. Can be used to determine the number of records in a machine
val repar = splitData.partitionByRange(0).mapPartition{in => in.map{(_,1)}}
>>> Custom Partitioning: Assigns records based on a key to a specific partition using a custom partitioner function. The key can be specified as a positioin key, an expression key, or a key selector function
class mypartitioner extends Partitioner[String] { def partition (s: String, i: Int): Int = {return 1}}
val repar = splitData.partitionCustom(new mypartitioner, 0).mapPartition{in => in.map{(_,1)}}
>>> Sort partition: Locally sorts all partitions of a dataset on a specified field in a specified order. Fields can be specified as tuple positions or field expressions. Sorting on multiple fields is done by chaining sortPartition() calls
val repar = splitData.sortPartition(1, Order.ASCENDING).mapPartition{in => in.map{(_,1)}}

## Partitioning handson
>> We will use the data in OnlineRetail.csv. Filter out the header row and also make sure every row has eight columns seperated with commas, else skip the row
val dataSet = benv.readTextFile("/work/data/OnlineRetail.csv")
val goodrecords = dataSet.filter(!_.startWith("InvoiceNo")).filter(line => line.split(,).length == 8)
goorecords.count()
val splitData = goodrecords.map(line => (line.split(",")(2).trim(), line.split(",")(3).trim().toInt))
splitData.first(3).print()
>> eg: output
>> (Heart Holder, 6)
>> (Coat Hanger, 8)
>> (Water Bottle, 6)

### Hash Partitioning
>> Paritioning the data by first column (description column)
val repar = splitData.partitionByHash(0).mapPartition{in => in.map{(_,1)}}
>> Count the number of elements in each partition to understand how the partitioning is hapenning
splitData.setParallelism(5).countElementsPerPartition.collect

>> eg: output (partitionNumber, valuesCount) --> Seq[(Int, Long)] = Buffer((0, 12755), (1,11619), (2,12284), (3,11876), (4,13026))
splitData.partitionByHash(0).setParallelism(5).countElementsPerPartition.collect

### Range Partitioning
val repar = splitData.partitionByRange(0).mapPartition{in => in.map{(_,1)}}
splitData.setParallelism(5).countElementsPerPartition.collect
splitData.partitionByRange(0).setParallelism(5).countElementsPerPartition.collect

### Sort Partition
val repar = splitData.sortPartition(1, Order.ASCENDING).mapPartition{in => in.map{(_,1)}}
splitData.sortPartition(1, Order.ASCENDING).setParallelism(5).countElementsPerPartition.collect
splitData.setParallelism(5).countElementsPerPartition.collect

### Custom partitioning
### partiton on string (description) column
class mypartitioner extends Partitioner[String] { def partition(s: String, i:Int): Int = {return math.abs(s.hashCode%5)}}

### partiton on interger column
class mypartitioner2 extends Partitioner[Int] { def partition(s: Int, i:Int): Int = {return math.abs(s.k%5)}}

val repar = splitData.partitionCustom(new mypartitioner,0).mapPartition{in => in.map{(_,1)}}
splitData.partitionCustom(new mypartitioner,0).setParallelism(5).countElementsPerPartition.collect
splitData.setParallelism(5).countElementsPerPartition.collect

# Chapter4: Batch Analytics with apache flink - Aggregations and Joins
# Data 1
>> The taxes dataset has two columns: State and TaxRate
val taxes = benv.readTextFile("/work/data/statesTaxRates.csv")
taxes.count()
taxes.first(3).print()
>> Output
>>> State, TaxRate
>>> Alabama, 4.00
>>> Arizona, 5.60 
val taxesColumns = taxes.filter(!_.startWith("State")).map(line => (line.split(",")(0).trim(), line.split(",")(1).trim().toFloat))
taxesColumns.first(3).print()
>> Output
>>>(Alabama,4.0)
>>>(Arizona,5.6)
>>>(Arkansas,6.5)

# Data 2
val populations = benv.readTextFile("/work/data/statesPopulation.csv")
populations.count()
populations.first(3).print()
>> output
>>>State,Year,Population
>>>Alabama,2010,4785492
>>>Alaska,2010,714031

val populationsColumns = populations.filter(!_.startWith("State")).map(line => (line.split(",")(0).trim(), line.split(",")(1).trim().toInt, line.split(",")(2).trim().toLong))
populationsColumns.first(3).print()
>> output
>>>(Alabama,2010,4785492)
>>>(Alaska,2010,714031)
>>>(Arkansas,2010,2921995)

# Group By: groups rows by a specific column
>> group by state and sum the population (3rd column)
populationsColumns.groupBy(0).sum(2).first(2).print()
>>Output
>>>(Alabama,2016,33771238)
>>>(Alaska,2013,5121020)

# Aggregation functions apply the function on a specific column
populationsColumns.filter(_._1 == "Alabama").sum(2).first(2).print()
populationsColumns.filter(_._1 == "Alabama").min(2).first(2).print()
populationsColumns.filter(_._1 == "Alabama").max(2).first(2).print()

# Reduce
>> Reduce applies a reduce function on a grouped tuples. Applied on a grouped dataset, it reduces each group to a single element using a user-defined reduce function
val reduce1 = populationsColumns.groupBy(0).reduce(new ReduceFunction[(String, Int, Long)]){
    override def reduce(intermediateResult: (String, Int, Long), next: (String, Int, Long)): (String, Int, Long) = {
        (intermediateResult._1, intermediateResult._2, intermediateResult._3 + next._3)
    }
})
reduce1.first(2).print()
>> output
>>>(Alabama,2016,33771238)
>>>(Alaska,2013,5121020)

# Reduce Group
>> Reduce group can be used to completely customize how aggregation is done. All values are passed to a list, allowing us to iterate and generate custom output
>> The output collected is also customizable to a new data type, such as tuples and so on
>> When applied on a grouped dataset, it calls a user-defined group-reduce function for each group
>> The difference between this and reduce is that the user-defined function gets the whole group at once
>> The function is invoked with an iterable over all elements of a group, and can return an arbitrary number of result elements
# TODO: Add example

# CoGroup
>> The cogroup transformation jointly processes groups of two datasets. Similar to reduce, group-reduce, and join, keys can be defined using the different key-selection methods. It is just like an outer join

# Joins
## TODO

# Chapter5: Stream Processing with Apache Flink - Transformations

 There is a producer always, which is putting events onto the messaging system. Then, there is a consumer which are consuming the events from the messaging system. There is a continuous stream of figures that is coming in and the consumers are consuming events marked yellow. The first thing it is consumed we turned it to green, the rest are still yellow. So, the consumer continuously consumes, which is processing all the events as and when it receives it. This is the basic principle behind any messaging system, message processing system, stream processing system, real-time processing system, real-time analytics. And now we will look at what are the different things that Flink is doing. Flink, if you put the consumer in Flink there are many things that need to happen for it to be reliable and then process everything. The first thing we want to understand is the parallelism concept. The parallelism is a of two types, one is data the other one is task. Flink can take the incoming data and partition the data across multiple machines in order to scale up the processing. The other way is to do task parallelism, where typically nowadays people have multiple CPUs anyway on multiple systems. So, you can have many cores and in some use cases you could use multiple CPUs to do multiple tasks at the same time all right. So, it's not like you one thing fits all, data parallelism versus task parallelism. It depends on your exact application you should be able to determine which one you would prefer more. 
 
 The Flink data streams offer the following advantages. 
 1) The first one is called exactly once processing. If you are processing your real-time system, because of the nature of the distributed systems which are serving as a message buses as well as a processing many many times you will see that the event is either processed one time or more times. It's called at least once, or sometimes the event might not even show up and you completely missed out, which is at most once. So, you don't want to have may or may not process or I will process it multiple times. Either way it is not a good idea. Exactly once means you are processing each event exactly once. You should go to a restaurant and order a coke you want exactly once, you don't want them to not give you a coke. You don't want them to give you two cokes, you just want one coke right. 
 2) The second one is called state management because many of these of the streaming applications, let's say Uber, you booked a ride on Uber. You have a state management, you need to be able to track it continuously so that you know exactly what's going on. So, Flink offers that very very well called state management. 
 3) The third on is handling of out-of-order data, that's a huge huge thing. If you look at other systems such as Apache Spark, only recently Spark is able to compete at this level with Flink in terms of how well it does out of order data when you deal with real-time streams. Flink has been doing it from the get-go. The handling of our of our data is important because not everything comes in exactly the same sequence. Let's say the time series or if you look at time stamps, you would like to have out of order processing capability. 
 4) The fourth one is flexible windowing. But the way to understand window is when you look at real-time it is pretty much an infinite unbounded stream of events. So, you might want to look at typically in terms of window T, which T is in seconds or milliseconds but you typically look at a window. You don't look at the entire thing because entire thing has no concept of beginning or end, it just goes on 24 by 7, 365 days a year forever. So, you need to create a window for which you want to measure something, like stock prices in the last 15 minutes, how many cars are going through a tollbooth in last 30 minutes or one hour or one day. It cannot be like how many cars right, so what would you do with that. 
 5) Checkpointing make sure that the state is maintained very well, any problems that you have with the entire system, like you shut down the system or something else happen, the Flink system will be able to recover from many many errors like that.

The next important concept I want to go through is called the shuffling or data exchange. There are many different types of data exchanges that happen because we are dealing with multiple machines and multiple tasks and there's a stream of events coming in non-stop. There are four different ways that you can look at. 
1) Let's look at the first one, forward or one to one. 
2) The second thing is called broadcast. Due to some reason in some use cases you might want to send all the data to all the machines. 
3) Hash key is the third one, where you redistribute the data of the streams according to some kind of a hash key or a key based on something from the event itself. So, you can redistribute the events and do a nice little scaling or distributed processing. You're just guaranteed that using hash key you collect, you guarantee that everything that belongs to a specific hash key, such as let's say the state in the US California, New York, or Rhode Island, or Connecticut they all go to the same task.  4) The fourth one is called rebalance, which is pretty much random because you just say okay I want to rebalance it because you don't know what's really going on, you're getting a bunch of events, and then you say rebalance so that kind of tests a random repartition in, such that every machine has same number of events. 

Let's quickly look at the three types of getting the data in. 

1) 
The first one is called socket base streaming where we will look at how to simply connect through a socket and just get some words, and our intent here is to simply get a workout.

>> get input data by connecting to the socket
val text = senv.socketTextStream("127.0.0.1", 9000, '\n')
>> parse the data, group it, window it, and aggregate the counts

val windowCounts = text.flatMap { w => w.split("\\s")}.map { w => WordWithCount(w,1)}.keyBy("word").timeWindow(Time.seconds(5), Time.seconds(1)).sum("count")
windowCounts.print().setParallelism(1)
senv.execute("Socket Window WordCount")


NC is a simple application that you can install on any Linux machine or even inside a Docker container which has Linux.
>> Start ncl server
nc -l 9000

Type in some words afer starting ncl server. That word will come in as a real-time stream to flink and this is going to continuously count it. 

2)
val text = senv.readFileStream("file:///work/data/streaming1.log")
case class WordWithCount(word: String, count: Long)
val windowCounts = text.flatMap { w => w.split("\\s")}.map { w => WordWithCount(w,1)}.keyBy("word").timeWindow(Time.seconds(5), Time.seconds(1)).sum("count")
windowCounts.print().setParallelism(1)
senv.execute("File Stream Word Count")

File based streaming. Here, you are getting the data by reading a file stream streaming1.log. We just streamed the file and it applied the aggregation and the window function on top. 

3) 
>> place following kafka jars under flink-1.8.0/lib folder
>> flink-connector-kafka-0.9_2.11-1.8.0.jar, flink-connector-kafka-0.10_2.11-1.8.0.jar , flink-connector-kafka-0.11_2.11-1.8.0.jar, flink-connector-kafka-base_2.11-1.8.0.jar, flink-dist_2.11-1.8.0.jar, kafka-clients-2.2.0.jar, log4j-1.2.17.jar, slf4j-log4j12-1.7.15.jar

./bin/start-scala-shell.sh local

# Kafka bootstrap script
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import java.util.properties

val properties = new Properties()
properties.setProperty("bootstrap.servers", "localhost:9092")
properties.setProperty("group.id", "test")
val myConsumer = new FlinkKafkaConsumer011[String](
    "flink1", new SimpleStringSchema, properties
)
val stream = senv.addSource(myConsumer)

case class WordWithCount(word: String, count: Long)
val windowCounts = text.flatMap { w => w.split("\\s")}.map { w => WordWithCount(w,1)}.keyBy("word").timeWindow(Time.seconds(5), Time.seconds(1)).sum("count")
windowCounts.print().setParallelism(1)
senv.execute("Kafka stream Word Count")

The third example we will see is Kafka based trimming. So, when you actually start this shell then kafka is automatically loaded. 

So, there's a bootstrap server, there's a Kafka concept, and then you start a Kafka consumer and that's your stream, that's it. After this, you take the stream, you apply the same kind of code. 

# Transformations on a data stream
./bin/start-scala-shell.sh local

nc -l 9000
nc -l 9001

val text = senv.socketTextStream("127.0.0.1", 9000, '\n')
## filter
val windowCounts = text.filter(!_.contains("ignore")).flatMap { w => w.split("\\s")}.map { w => WordWithCount(w,1)}.keyBy("word").timeWindow(Time.seconds(5), Time.seconds(1)).sum("count")
## map
val windowCounts = text.map { w => WordWithCount(w,1)}.keyBy("word").timeWindow(Time.seconds(5), Time.seconds(1)).sum("count")
## flatmap
val windowCounts = text.flatMap { w => w.split("\\s")}.map { w => WordWithCount(w,1)}.keyBy("word").timeWindow(Time.seconds(5), Time.seconds(1)).sum("count")
## Aggregation (min,max,sum)
val windowCounts = text.flatMap { w => w.split("\\s")}.map { w => WordWithCount(w,1)}.keyBy("word").timeWindow(Time.seconds(5), Time.seconds(1)).min("count")

windowCounts.print().setParallelism(1)
senv.execute("Socket Window WordCount")

# Partitioning
>> Rescale: Partitions elements in a round-robin fashion to a subset of downstream operations: stream.rescale()
>> rebalance: Partitions elements in a round-robin fashion, creating an equal load per partition; stream.rebalance()
>> shuffle: Partitions elements randomly according to a uniform distribution, stream.shuffle(), and hence is also used to avoid malfunctioning of the system
>> broadcast: Broadcasts elements to every partition; stream.broadcast()
>> partitionCustom: uses a user-defined partitioner to select the target task for each element; stream.partitionCustom(partitioner, 0)

# Window Operations
## Windows
>> Time-driven windows: windows every T seconds
val windowCounts = text.map { w => WordWithCount(w,1)}.keyBy("word").timeWindow(Time.seconds(5), Time.seconds(1)).sum("count")
>> Data driven windows: Window every N elements
val windowCounts = text.map { w => WordWithCount(w,1)}.keyBy("word").countWindow(5).sum("count")
>> Tumbling windows: No overlapping ( for example - five seconds window)
>> Sliding windows: With overlapping ( for example - last five seconds)
>> Session Windows: Based on activity ( for example - during a user session)

# Time Characteristics 
## Watermarks & Times
>> Watermarks is similar to let's say you're pouring juice or water from a jug into another glass, so there is a watermark. You know till what you are pouring it. using the watermarks is very easy for you to recover and then do the needful processing. So, watermarks flow is part of the data stream.
>> Watermark: The mechanism in flink to measure progress in event time is watermarks. Can be used for data recovery. Flows as a part of the data stream and carries a timestamp , "t". Generated at, or directly after, source functions. Each parallel subtasks of a source function usually generates its watermarks independently. These define the event time at that particular parallel source. Each and every watermark is marked seperately. 
>> Event Time: Time when the event was created at the source system
>> Ingestion time: Time when the event enters the dataflow
>> Processing time: Time when the event was processed

### Slidding Window
senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
// get input data by connecting to the socket
val text = senv.socketTextStream("127.0.0.1", 9000, '\n').flatMap { w => w.split("\\s")
// assigning timestamp and watermark. Map each word to WordTimeStampCount object. Assinging current time and count of 1 to every word.
.map { w => WordTimeStampCount(w,System.currentTimeMillis(), 1)}
// assigns the timestamp returned by TimestampAssigner as watermark
.assignTimestampsAndWatermarks(new TimestampAssigner())

// parse the data, group it, window it, and aggregate the counts
val windowCounts = text.keyBy("word")
// window words in last 5 seconds
.window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
// each word has count 1, but sum aggregates the count of word in last 5 seconds
.sum("count")
// print the results with a single thread, rather than in parallel
WindowCounts.print().setParallelism(1)
senv.execute("Socket Window WordCount")


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.functions.timestamps._

// Data type for words with count
case class WordTimeStampCount(word: String, timestamp: Long, count: Long)


// assigns the time of wordtimestampcount as the event time used by watermark
class TimestampAssigner extends AscendingTimestampExtractor[WordTimeStampCount] {
    override def extractAscendingTimestam(input: WordTimeStampCount): Long = {
        return input.timestamp;
    }
}

// Exiting paster mode, now interpreting.
WordTimeStampCount(a, 1560520518205, 1)
WordTimeStampCount(a, 1560520518205, 1)

### Tumbling window
.window(TumblingEventTimeWindows.of(Time.seconds(5)))

// example
env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

DataStream<Message> messageStream = eventsStream
            .keyBy(event -> event.getId())
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .trigger(PurgingTrigger.of(ProcessingTimeTrigger.create()))
            .apply(new WindowFunction).
            .setParallelism(2);

### Session Window
.window(EventTimeSessionWindow.withGap(Time.seconds(10)))

### Ingestion Time
env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

.window((SessionIngestionTimeWindows.of(Time.seconds(5)))

# State and Checkpointing
## State
>> Every funciton and operator in Flink can be stateful
>> Stateful functions store data across the processing of individual elements/events, making state a critical building block for any type of elaborate operation
>> By default, state is kept in the memory in the TaskManagers, and checkpoints are stored in the memory in the JobManager

## Checkpointing
>> The state needs to be made fault tolerant and for this, Flink needs to checkpoint the state
>> Checkpointing allows flink to recover state and positions in the streams to give the application the same semantics as a failure free execution
senv.enableCheckpointing(n), where n is the checkpoint interval in milliseconds
senv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
senv.getCheckpointConfig.setCheckpointTimeout(60000)
senv.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
senv.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
senv.getCheckpointConfig.setFailTasksOnCheckpointingErrors(false)

// example
import org.apache.flink.runtime.state.filesystem._
import org.apache.flink.streaming.api._
senv.setStateBackend(new FsStateBackend("file:///work/data/checkpoints")
senv.enableCheckpointing(1000)
senv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
senv.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)

// Data type for words with count
case class WordWithCount(word: String, count: Long)
// get input data by connecting to the socket
val text = senv.socketTextStream("127.0.0.1", 9000, '\n')
// parse the data, group it, window it, and aggregate the counts
val windowCounts = text.flatMap { w => w.split("\\s") }.map{ w => WordWithCount(w,1) }.keyBy("word").timeWindow(Time.seconds(5), Time.seconds(1)).sum("count")

## Sink
>> Provides a sink that writes partitioned files to filesystems, supported by the Flink FileSystem abstraction
>> The streaming file sink writes data into buckets, which are configurable
>> Witihin a bucket, we further split the output into smaller part files based on a rolling policy

val sink: StreamingFileSink[String] = StreamingFileSink.forRowFormat(new Path(outputPath), new SimpleStringEncoder[String]("UTF-8").build()
val text = senv.socketTextStream("127.0.0.1", 9000, '\n')
case class WordWindowCount(word: String, count: Long)
val windowsCounts = text.flatMap { w => w.split("\\s") }.map{ w => WordWithCount(w,1) }.keyBy("word").timeWindow(Time.seconds(5), Time.seconds(1)).sum("count")
.setParallelism(1).addSink(sink)

senv.execute("Socket Window WordCount")

### Joining 2 Streams
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.functions.timestamp._

case class WordTimeStampCount(word: String, streamLabel: String, timestamp: Long, count: Long)
senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

class TimestampAssigner extends AscendingTimestampExtractor[WordTimeStampCount] {
    override def extractAscendingTimestamp(input: WordTimeStampCount): Long = {
        return input.timestamp;
    }
}

val inputA = senv.socketTextStream("127.0.0.1", 9000, '\n')
val inputB = senv.socketTextStream("127.0.0.1", 9001, '\n')

val streamA = intpuA.flatMap { w => w.split("\\s").map { w => WordTimeStampCount(w, "streamA" System.currentTimeMillis(), 1)}.assignTimestampsAndWatermarks(new TimestampAssigner())
val streamB = inputB.flatMap { w => w.split("\\s").map { w => WordTimeStampCount(w, "streamB", System.currentTimeMillis(), 1)}.assignTimestampsAndWatermarks(new TimestampAssigner())

val outputA = streamA.keyBy("word").window(TumblingEventTimeWindows.of(Time.seconds(5))).sum("count")
val outputB = streamB.keyBy("word").window(TumblingEventTimeWindows.of(Time.seconds(5))).sum("count")

outputA.print().setParallelism(1)
outputB.print().setParallelism(1)

val joinedStream = outputA.join(outputB)
.where(_.word).equalTo(_.word)
.window(TumblingEventTimeWindows.of(Time.seconds(5)))
.apply{ (w1, w2) => WordTimeStampCount(w1.count, "joinedStream", System.currentTimeMillis(), w1.count + w2.count)}

joinedStream.print().setParallelism(1)
senv.execute("Socket Window WordCount")