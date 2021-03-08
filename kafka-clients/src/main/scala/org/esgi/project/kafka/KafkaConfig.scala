package org.esgi.project.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.esgi.project.kafka.MessageProcessing.applicationName

import java.io.InputStream
import java.util.Properties

trait KafkaConfig {
  // auto loader from properties file in project
  def buildProducerProperties: Properties = {
    val inputStream: InputStream = getClass.getClassLoader.getResourceAsStream("kafka.properties")
    val properties = new Properties()
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, applicationName)
    properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    properties.load(inputStream)
    properties
  }

  def buildConsumerProperties: Properties = {
    val inputStream: InputStream = getClass.getClassLoader.getResourceAsStream("kafka.properties")
    val properties = new Properties()
    properties.put(ConsumerConfig.CLIENT_ID_CONFIG, applicationName)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, applicationName)
    properties.load(inputStream)
    properties
  }
}
