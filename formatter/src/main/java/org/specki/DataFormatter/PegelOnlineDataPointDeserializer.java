package org.specki.DataFormatter;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class PegelOnlineDataPointDeserializer implements Deserializer<PegelOnlineDataPoint> {


	@Override public void configure(Map<String, ?> arg0, boolean arg1) {}

	@Override public PegelOnlineDataPoint deserialize(String arg0, byte[] arg1) {
		ObjectMapper mapper = new ObjectMapper();
		PegelOnlineDataPoint data = null;

		try {
			data = mapper.readValue(arg1, PegelOnlineDataPoint.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return data;
	}

	@Override
	public void close() {}
}
