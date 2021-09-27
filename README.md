# Normal Requirement
* Receive message from Kafka
* Access database
* Send message to Kafka

# Test Requirement
* Start Kafka and PostgreSQL
* Send message to service
* Wait for message
* Verify database data

# Tools
* Flyway - DB migration
* Testcontainer - Kafka container and PostgreSQL container
* SpringBoot test

# Example
* The service 
  * listen to the Kafka requestTopic
  * Persist Person data
  * send the persisted ID to the Kafka responseTopic
* Test
  * send message to Kafka requestTopic
  * Consume message from Kafka responseTopic
  * Verify Person data in DB
