from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

if __name__ == "__main__":

	#Create Spark Context to Connect Spark Cluster
    sc = SparkContext(appName="Assigmnent4")
	#Set the Batch Interval is 10 sec of Streaming Context
    ssc = StreamingContext(sc, 10)

	#Create Kafka Stream to Consume Data Comes From Twitter Topic
    kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'twitter':1})

    ######################################################################
    ######################################################################
    #>>>>>>>>>>>>>>>>>>>>>>>> START your code here <<<<<<<<<<<<<<<<<<<<<<#
    ######################################################################
    lines= kafkaStream.map(lambda x: x[1])
    tweet_cs = lines.map(lambda x: json.loads(x)["text"])
    
    #*cs=Context_Stream
    ######################################################################
    #>>>>>>>>>>>>>>>>>>>>> Print the first 5 tweets <<<<<<<<<<<<<<<<<<<<<#
    ######################################################################	
    tweet_cs.pprint(5)

    ######################################################################
    #>>>>>>>>>>>> Print tweets that contain the word "USA" <<<<<<<<<<<<<<#
    ######################################################################
    tweet_cs.filter(lambda x: '%USA%' in x).pprint()

    ######################################################################
    #>>>>>>>>>>>>>>>>>>>>>>>>END your code here<<<<<<<<<<<<<<<<<<<<<<<<<<#
    ######################################################################
    ######################################################################
    
    #Start Execution of Streams
    ssc.start()
    ssc.awaitTermination()