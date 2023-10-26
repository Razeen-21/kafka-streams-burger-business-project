# kafka-streams-burger-business-project
A kafka streams use case for a business data processing involving aggregating revenue and order counts by meat type, store codes or both.

The project leverages the java kafka streams library to build topologies that can do these aggregations for us.

When cloning and starting the application, please note the following terminal commands to get started:

Commands to initialize Kafka environment (cd to your version of kafka installed on your machine, I used kafka_2.13-3.5.0)

To initialize Zookeeper: bin/zookeeper-server-start.sh config/zookeeper.properties

To initialize local Kafka broker (server): bin/kafka-server-start.sh config/server.properties

To run the project as a single instance, simply click the run application button in IntelliJ.

To run the project as 2 instances, please run the following commands:

./gradlew clean build 
or 
./gradlew clean build -x test (if the tests are giving you issues) 

To run the jar file on different ports:

java -jar build/libs/kafka-streams-demo-project-0.0.1-SNAPSHOT.jar (on port 8080)
java -jar -Dserver.port=8081 build/libs/kafka-streams-demo-project-0.0.1-SNAPSHOT.jar (on port 8081)

and there you have it, 2 Kafka instances running on two different ports



