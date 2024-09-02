from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# sc = SparkContext("local[*]", "NetworkWordCount")
# ssc = StreamingContext(sc,1)
#
# lines = ssc.socketTextStream("localhost", 9999)

# words = lines.flatMap(lambda line : line.split(" "))
# pairs = words.map(lambda word : (word, 1))
# wordcounts = pairs.reduceByKey(lambda x,y : x + y)
# wordcounts.pprint()

# ssc.start()
# ssc.awaitTermination()

spark = SparkSession\
         .builder\
        .appName("kafka test")\
    .getOrCreate()

print(spark)

df= (spark.read
.format("kafka")
     .option("kafka.bootstrap.servers", "localhost:9092")
     .option("subscribe", "quickstart-events")
     # .option("startingOffsets", "from-beginning")
     .load())

df.show()