package com.tkmtwo.ps2j;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

/**
 * POJO class used with Spring's JSON serdes.
 *
 * Using Jackson annotations, we will get the main pubsub message 
 * as a POJO.
 *
 */
public class PubSubMessage {

		@JsonProperty("MessageData")
		private String messageData;

		@JsonProperty("AttributesMap")
		private Map<String, String> attributesMap;



		public String getMessageData() { return messageData; }
		public void setMessageData(String s) { messageData = s; }
		

		public Map<String, String> getAttributesMap() { return attributesMap; }
		public void setAttributesMap(Map<String, String> m) {attributesMap = m; }
		
		public String toString() {
				StringBuilder sb = new StringBuilder();
				sb
						.append("MyMessageData=")
						.append(getMessageData());
				sb.append(" attributesMap has " + getAttributesMap().size());
				return sb.toString();
		}
		
		
}



