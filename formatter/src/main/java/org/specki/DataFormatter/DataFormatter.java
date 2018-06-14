package org.specki.DataFormatter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.util.StringUtils;


@Configuration
@EnableKafka
@EnableKafkaStreams
public class DataFormatter {

@Autowired private KafkaProperties kafkaProperties;
@Autowired private StreamsBuilder builder;
	
	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public StreamsConfig kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "HygonAggregator");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return new StreamsConfig(props);
	}
	
	@Bean
	KStream <byte[], String> formatPegelOnlineStream(StreamsBuilder builder) {
	// Input Stream
	KStream<byte[], String> dataStream = builder.stream("PegelOnlineRAW");
	// Output Stream 
	KStream<String, PegelOnlineDataPoint> formattedStream = dataStream.flatMap(
			(KeyValueMapper<? super byte[], ? super String, ? extends Iterable<? extends KeyValue<String,PegelOnlineDataPoint>>>)
				(k ,v) -> {
					List<KeyValue<String, PegelOnlineDataPoint>> result = new ArrayList<>();
					JSONArray allStations;
					
					// Remove noise & Bit hacking cause it somehow gets broken in the process
					v = StringUtils.trimAllWhitespace(v);
					v = StringUtils.deleteAny(v, "\n").substring(7);
					
					// Convert to JSON Object and extract Data into custom Format
					try {
						allStations = new JSONArray(v);
						for (int i = 0; i < allStations.length(); i++) {
							JSONObject element = allStations.getJSONObject(i);
							String key = element.getString("longname");
							
							PegelOnlineDataPoint dp = new PegelOnlineDataPoint();
							dp.setLon(element.getLong("longitude"));
							dp.setLat(element.getLong("latitude"));
							
							result.add(new KeyValue<>(key, dp));
						}
					} catch (JSONException e) {
						// Data Point is discarded.
					}
		             return result;
				}
			);

	 formattedStream.to("PegelOnlineData",
			 Produced.with(
					 Serdes.String(),
					 Serdes.serdeFrom(new PegelOnlineDataPointSerializer(), new PegelOnlineDataPointDeserializer()))
			 );
	 return dataStream;
	}

	@Bean
	KStream <String,String> preformatHygonStream(StreamsBuilder builder) {
		// Input Data Stream
		KStream<byte[], String> rawdataStream = builder.stream("HygonWLRaw");
		// Preformats Stations Stream to use Station Name as Key
		KStream<String, String> preformattedDataStream = rawdataStream.map(
				(KeyValueMapper<byte[], String, KeyValue<String,String>>)
				(k, v) -> {
					String key = v.split(";")[0];
					return new KeyValue<>(key.toUpperCase(), v);
				}
		);
		preformattedDataStream.to("PreformattedHygonData",
				Produced.with(
						Serdes.String(),
						Serdes.String())
				);

		// Input Stations Stream
		KStream<byte[], String> rawStationsStream = builder.stream("HygonStationsWL");	
		// Preformats Stations Stream to use Station Name as Key
		KStream<String, String> preformattedStationsStream = rawStationsStream.map(
				(KeyValueMapper<byte[], String, KeyValue<String,String>>)
				(k, v) -> {
					String key = v.split(";")[0];
					return new KeyValue<>(key.toUpperCase(), v);
				}
		);
		preformattedStationsStream.to("PreformattedHygonStations", 
				Produced.with(
						Serdes.String(),
						Serdes.String())
				);
		return preformattedStationsStream;
	}
	

	@Bean
	KStream <String, HygonDataPoint> formatHygonStream(StreamsBuilder builder) {

		// Reimport Stations as Table
//		KTable<String,String> stationsTable = builder.table("PreformattedHygonStations", Consumed.with(Serdes.String(), Serdes.String()));
//		KStream<String,String> hygonDataStream = builder.stream("PreformattedHygonData", Consumed.with(Serdes.String(), Serdes.String()));
		/*
		KStream<String, HygonDataPoint> outputStream = hygonDataStream.join(stationsTable, 
				(ValueJoiner<String,String,HygonDataPoint>)(data, station) -> {
					try {
						System.out.println(data);
						System.out.println(station);
						
						
//						String[] dataArray = data.split(";");
//						String[] stationArray = station.split(";");
						
						// Stations: Name;Gewaesser;MNW;MHW;Mittel;Informationsstufe 1;Informationsstufe 2;Informationsstufe 3;Einzugsgebiet;east;north
						HygonDataPoint dp = new HygonDataPoint();
//						dp.setMeasurement(Float.parseFloat(dataArray[3]));
//						dp.setMnw(Float.parseFloat(stationArray[2]));
//						dp.setMhw(Float.parseFloat(stationArray[3]));
//						dp.setLevel1(Float.parseFloat(stationArray[4]));
//						dp.setLevel2(Float.parseFloat(stationArray[5]));
//						dp.setLevel3(Float.parseFloat(stationArray[6]));
//						//TODO: Parse East/North to Lat/Lon
//						System.out.println(dp.toString());
						return dp;
					} catch (Exception e) {
						e.printStackTrace();
						return null;
					} 
				}
		);
		*/
		//outputStream.print();
		//outputStream.to("HygonData");
		return null; // outputStream;
	}

}
