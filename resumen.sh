#TOPIC

# Create topic 
k-topic --topic first_topic --create --partitions 3 --replication-factor 1

# List topics
k-topic --list 

# Describe topic
k-topic --topic first_topic --describe

# Delete topic 
k-topic --topic first_topic --delete

#PRODUCER

# create producer
k-producer --topic first_topic 

# create producer with acks
k-producer --topic first_topic --producer-property acks=all

# create producer with key
k-producer --topic first_topic --property parse.key=true --property key.separator=:

# create producer with round-robin
k-producer --producer-property partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner --topic first_topic

#CONSUMER

# create consumer
k-consumer --topic first_topic

# create consumer from beginning
k-consumer --topic first_topic --from-beginning

# create consumer with display key, values and timestamp 
k-consumer --topic first_topic --formatter kafka.tools.DefaultMessageFormatter --property print.timestamp=true --property print.key=true --property print.value=true --property print.partition=true --from-beginning

# create consummer with group
k-consumer --topic first_topic --group my-first-application

