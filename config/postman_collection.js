{
	"variables": [],
	"info": {
		"name": "OpenEOI",
		"_postman_id": "65203293-8996-aba9-5667-4ff10626697a",
		"description": "",
		"schema": "https://schema.getpostman.com/json/collection/v2.0.0/collection.json"
	},
	"item": [
		{
			"name": "GET Available Connector Types",
			"request": {
				"url": "http://localhost:8083/connector-plugins",
				"method": "GET",
				"header": [],
				"body": {},
				"description": "Gets all currently registered Connectors"
			},
			"response": []
		},
		{
			"name": "POST Pegelonline Connector",
			"request": {
				"url": "http://localhost:8083/connectors",
				"method": "POST",
				"header": [
					{
						"key": "Accept",
						"value": "application/json",
						"description": ""
					},
					{
						"key": "Content-Type",
						"value": "application/json",
						"description": ""
					},
					{
						"key": "Host",
						"value": "",
						"description": "",
						"disabled": true
					}
				],
				"body": {
					"mode": "raw",
					"raw": "{\n  \"name\": \"PegelOnline Stations Connector\",\n  \"config\": {\n    \"connector.class\": \"com.tm.kafka.connect.rest.RestSourceConnector\",\n    \"tasks.max\": \"1\",\n    \"rest.source.poll.interval.ms\": \"300000\",\n    \"rest.source.method\": \"GET\",\n    \"rest.source.url\": \"https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations.json?includeTimeseries=true&includeCurrentMeasurement=true\",\n    \"rest.source.payload.converter.class\": \"com.tm.kafka.connect.rest.converter.StringPayloadConverter\",\n    \"rest.source.properties\": \"Content-Type:application/json,Accept::application/json\",\n    \"rest.source.topic.selector\": \"com.tm.kafka.connect.rest.selector.SimpleTopicSelector\",\n    \"rest.source.destination.topics\": \"PegelOnlineData\"\n  }\n}"
				},
				"description": ""
			},
			"response": []
		},
		{
			"name": "GET Active Connectors",
			"request": {
				"url": "http://localhost:8083/connectors",
				"method": "GET",
				"header": [],
				"body": {},
				"description": ""
			},
			"response": []
		}
	]
}