# Twitter-Hashtags

-> This app gives user an access to collect information over a topic of his interest that he wants to analyze which in trending in twitter.

-> This app streams over tweets with the User's input and collects out all the streams related to that.

-> Kafka puts those streams into Kafka topic using a Kafka Producer.

-> Kafka Consumer(Multiple instances of consumer will run to allow parallel consumption from the topic to speed up the process) will consume the data from Kafka Topic.

-> The data will be stored in ElasticDB which can be further collected from there and can be analyzed using ELK stack.

