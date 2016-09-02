
Copy the data file to here:

/user/user01/data/sms-call.txt

- Create topic in streams or kafka

maprcli stream create -path /user/user01/stream -produceperm p -consumeperm p -topicperm p
maprcli stream topic create -path /user/user01/stream -topic cdrs -partitions 3
maprcli stream topic create -path /user/user01/stream -topic cdrp -partitions 3

To run the MapR Streams Java producer:

java -cp mapr-streams-spark-1.0.jar:`mapr classpath` com.streamskafka.example.MsgProducer /user/user01/stream:cdrp /user/user01/data/sms-call.txt

To Run the Spark Consumer Producer (in separate consoles if you want to run at the same time):

spark-submit --class com.sparkkafka.example.SparkKafkaConsumer --master local[2] mapr-streams-spark-1.0.jar localhost:9999 /user/user01/stream:cdrs /user/user01/stream:cdrp

To run the MapR Streams Java consumer:

java -cp mapr-streams-spark-1.0.jar:`mapr classpath` com.streamskafka.example.MsgConsumer /user/user01/stream:cdrp
      
 - For Yarn you should change --master parameter to yarn-client - "--master yarn-client"