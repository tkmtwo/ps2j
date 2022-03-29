package com.tkmtwo.ps2j;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.apache.kafka.clients.consumer.ConsumerRecord;

//import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import java.util.Map;

import org.unbescape.json.JsonEscape;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;


@SpringBootApplication
public class Ps2jApplication {

		//
		//Configuration from application.properties
		//
		
		@Value("#{${demux.topic.map}}")		
		Map<String, String> demuxTopicMap;

		@Value("${demux.discriminator.path}")
		String demuxDiscriminatorPath;

		@Value("${sink.topic:#{null}}")
		String sinkTopic;



		//
		// An auto-wired template for publishing.
		//
		@Autowired
		private KafkaTemplate<String, String> template;

		//
		// A standard jackson ObjectMapper for use later
		//
		private ObjectMapper objectMapper = new ObjectMapper();
		
		
		
		
		public static void main(String[] args) {
				SpringApplication.run(Ps2jApplication.class, args);
		}
		



		
		/**
		 * Send a key and value to a topic.
		 *
		 * This is the same method regardless of what topic we are sending to.
		 *
		 */
		private void sendMessage(String topicName, String key, String value) {
				template.send(topicName, key, value);
		}
		


		/**
		 * Given a raw message, determine the demuxed topic based on
		 * the discriminator path.
		 *
		 */		
		private String resolveTopic(JsonNode jn) {
				JsonNode ddNode = jn.path(demuxDiscriminatorPath);
				
				if (ddNode == null) { return null; }
				if (JsonNodeType.STRING != ddNode.getNodeType()) { return null; }
				
				String ddValue = ddNode.asText();
				
				if (demuxTopicMap.containsKey(ddValue)) {
						return demuxTopicMap.get(ddValue);
				}
				
				return null;
		}
		
		
		/**
		 * The PubSub message consumer.
		 *
		 * This is the one "incoming" topic.
		 * Messages on this topic are assumed to be the out-of-box messages
		 * as sourced via the Confluent GCP PubSub Source connector.
		 *
		 * Incoming topic name is in the application properties as "consumer.topic"
		 * Consumer Group ID is in the application properties as "consumer.group"
		 *
		 */
		@Component
		class Consumer {
				
				@KafkaListener(topics = {"${consumer.topic}"}, groupId = "${consumer.group}")
				public void consume(ConsumerRecord<String, PubSubMessage> record) {
						
						//In terms of consuming the message, most of the work has already been done.
						//The ConsumerRecord we get will be a String key and a PubSubMessage POJO
						//that has been deserialized by the Spring framework converter.
						//See the jackson annotations in PubSubMessage.class

						//Now we pluck out the MessageData, read it as JSON, and demux to destination
						//topics.
						
						try {
								
								// First read the contents of "MessageData" into a general JsonNode
								JsonNode jn = objectMapper.readTree(record.value().getMessageData());
								
								//Try to resolve a demux topic, if we have configured it to do so
								String resolvedTopic = resolveTopic(jn);
								
								//Send to resolved topic if we got one
								if (resolvedTopic != null) {
										sendMessage(resolvedTopic, record.key(), jn.toString());
								}
								
								//Also send to sink topic if we got one
								if (sinkTopic != null) {
										sendMessage(sinkTopic, record.key(), jn.toString());
								}

								//I know...we did two things in this one method.
								//Technically, one could fail and not the other.
								//Additional robust handling is left as an excercise for
								//the next writer.
								
						} catch (Exception ex) {
								//Do something more meaningful. Perhaps an error topic.
								System.out.println("CAUGHT: " + ex.toString());
						}
						
				}
		}
		
		
		
		
}
