import findspark
findspark.init()
import pyspark

# import necessary packages
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc

sc = SparkContext()
# we initiate the StreamingContext with 10 second batch interval.
# next we initiate our sqlcontext
ssc = StreamingContext(sc, 10)
SQLContext = SQLContext(sc)

# initiate streaming text from a TCP (socket) source:
socket_stream = ssc.socketTextStream("127.0.0.1", 5555)
# lines of tweets with socket_stream window of size 60, or 60 #seconds windows of time.
lines = socket_stream.window(60)

# just a tuple to assign names.
from collections import namedtuple
fields = ("hashtag", "count")
Tweet = namedtuple('Tweet', fields)
# here we apply differet operations on the tweets and save them to a temporary sql table
# splits to a list
(lines.flatMap(lambda text: text.split(" "))
 # check for hashtag calls
 .filter(lambda word: word.olower().startswith('#'))
 # Lower cases the word.
 .map(lambda word: (word.lower(), 1))
 .reduceByKey(lambda a, b: a + b)
 # stores in a Tweet Object.
 .map (lambda rec: Tweet(rec[0], rec[1]))
 # sorts them in a dataframe
 .foreachRDD( lambda rdd: rdd.toDF().sort(desc("count"))
 #Registers only top 10 hashtags to a table.
 .limit(10).registerTempTable("tweets"))
)


