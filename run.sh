curl -X POST \
  http://localhost:8083/connectors \
  -H 'accept: application/json' \
  -H 'cache-control: no-cache' \
  -H 'content-type: application/json' \
  -H 'postman-token: ce659518-e57e-1d02-c5f2-fb5b205f2c15' \
  -d '{
  "name": "PegelOnline Stations Connector",
  "config": {
    "connector.class": "com.tm.kafka.connect.rest.RestSourceConnector",
    "tasks.max": "1",
    "rest.source.poll.interval.ms": "60000",
    "rest.source.method": "GET",
    "rest.source.url": "https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations.json?includeTimeseries=true&includeCurrentMeasurement=true",
    "rest.source.payload.converter.class": "com.tm.kafka.connect.rest.converter.StringPayloadConverter",
    "rest.source.properties": "Content-Type:application/json,Accept::application/json",
    "rest.source.topic.selector": "com.tm.kafka.connect.rest.selector.SimpleTopicSelector",
    "rest.source.destination.topics": "PegelOnlineRAW",
    "log4j.logger": "TRACE, kafkaAppender"
  }
}'

curl -X POST \
  http://localhost:8083/connectors \
  -H 'accept: application/json' \
  -H 'cache-control: no-cache' \
  -H 'content-type: application/json' \
  -H 'postman-token: 0625eb03-628b-03cb-e797-81c08711ea8a' \
  -d '{
  "name": "Hygon Data Connector",
  "config": {
    "connector.class": "com.tm.kafka.connect.rest.RestSourceConnector",
    "tasks.max": "1",
    "rest.source.poll.interval.ms": "60000",
    "rest.source.method": "GET",
    "rest.source.url": "http://luadb.it.nrw.de/LUA/hygon/messwerte/messwerte.tar.gz",
    "rest.source.payload.converter.class": "com.tm.kafka.connect.rest.converter.HYGONPayloadConverter",
    "rest.source.properties": "",
    "rest.source.topic.selector": "com.tm.kafka.connect.rest.selector.HygonTopicSelector",
    "rest.source.destination.topics": "HygonRAW",
    "log4j.logger": "TRACE, kafkaAppender"
  }
}'

curl -X POST \
  http://localhost:8083/connectors \
  -H 'accept: application/json' \
  -H 'cache-control: no-cache' \
  -H 'content-type: application/json' \
  -H 'postman-token: a483f20d-8585-7f91-0635-fa12f645fcb9' \
  -d '{
  "name": "Hygon Stations Connector",
  "config": {
    "connector.class": "com.tm.kafka.connect.rest.RestSourceConnector",
    "tasks.max": "1",
    "rest.source.poll.interval.ms": "60000",
    "rest.source.method": "GET",
    "rest.source.url": "http://luadb.it.nrw.de/LUA/hygon/messwerte/pegeldaten.tar.gz",
    "rest.source.payload.converter.class": "com.tm.kafka.connect.rest.converter.HYGONPayloadConverter",
    "rest.source.properties": "",
    "rest.source.topic.selector": "com.tm.kafka.connect.rest.selector.HygonTopicSelector",
    "rest.source.destination.topics": "HygonStationsWL",
    "log4j.logger": "TRACE, kafkaAppender"
  }
}'


cd target
java -jar DataFormatter-0.0.2-SNAPSHOT.jar
