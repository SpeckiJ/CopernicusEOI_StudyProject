package org.specki.DataFormatter;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

// Taken from https://dzone.com/articles/kafka-sending-object-as-a-message
public class PegelOnlineDataPointSerializer implements Serializer<PegelOnlineDataPoint> {

	@Override public void configure(Map map, boolean b) {}

	@Override public byte[] serialize(String arg0, PegelOnlineDataPoint arg1) {
		byte[] retVal = null;
		ObjectMapper objectMapper = new ObjectMapper();

		try {
			retVal = objectMapper.writeValueAsString(arg1).getBytes();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return retVal;
	}

	@Override public void close() {}
}