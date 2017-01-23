Install and fire up the Sandbox using the instructions here: http://maprdocs.mapr.com/home/SandboxHadoop/c_sandbox_overview.html. 

Use an SSH client such as Putty (Windows) or Terminal (Mac) to login. See below for an example:
use userid: user01 and password: mapr.

For VMWare use:  $ ssh user01@ipaddress 

For Virtualbox use:  $ ssh user01@127.0.0.1 -p 2222 


You can build this project with Maven using IDEs like Eclipse or NetBeans, and then copy the JAR file to your MapR Sandbox, or you can install Maven on your sandbox and build from the Linux command line.

You can use scp to copy your JAR file to the MapR Sandbox:

use userid: user01 and password: mapr.
For VMWare use:  $ scp  nameoffile.jar  user01@ipaddress:/user/user01/. 

For Virtualbox use:  $ scp -P 2222 nameoffile.jar  user01@127.0.0.1:/user/user01/.  

Copy the data file using scp to here:

/user/user01/data/sms-call.txt

- Create the topics to read from and write to in MapR streams:

maprcli stream create -path /user/user01/stream -produceperm p -consumeperm p -topicperm p
maprcli stream topic create -path /user/user01/stream -topic cdrs -partitions 3
maprcli stream topic create -path /user/user01/stream -topic cdrp -partitions 3

To run the MapR Streams Java producer to produce messages, run the Java producer with the topic and data file arguments:

java -cp mapr-streams-spark-1.0.jar:`mapr classpath` com.streamskafka.example.MsgProducer /user/user01/stream:cdrp /user/user01/data/sms-call.txt

To Run the Spark Consumer Producer (in separate consoles if you want to run at the same time) run the spark consumer with the topic to read from and write to:

spark-submit --class com.sparkkafka.example.SparkKafkaConsumer --master local[2] mapr-streams-spark-1.0.jar localhost:9999 /user/user01/stream:cdrp /user/user01/stream:cdrs

To run the MapR Streams Java consumer, run the Java consumer with the topic to read from:

java -cp mapr-streams-spark-1.0.jar:`mapr classpath` com.streamskafka.example.MsgConsumer /user/user01/stream:cdrp
      
 - For Yarn you should change --master parameter to yarn-client - "--master yarn-client"
