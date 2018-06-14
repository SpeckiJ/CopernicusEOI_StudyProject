read -p "Please supply your confluent install directory: " directory

$directory/bin/confluent stop

cd connector
mvn package
cp -r target/kafka-connect-rest-1.0-SNAPSHOT-package/share/java/. $directory/share/java/

$directory/bin/confluent start

$directory/bin/kafka-topics --zookeeper localhost:2181 \
   --topic PegelOnlineData --create \
   --replication-factor 1 --partitions 1

$directory/bin/kafka-topics --zookeeper localhost:2181 \
   --topic PegelOnlineRAW --create \
   --replication-factor 1 --partitions 1

$directory/bin/kafka-topics --zookeeper localhost:2181 \
   --topic PreformattedHygonData --create \
   --replication-factor 1 --partitions 1

$directory/bin/kafka-topics --zookeeper localhost:2181 \
   --topic PreformattedHygonStations --create \
   --replication-factor 1 --partitions 1

$directory/bin/kafka-topics --zookeeper localhost:2181 \
   --topic HygonStationsWL --create \
   --replication-factor 1 --partitions 1

$directory/bin/kafka-topics --zookeeper localhost:2181 \
   --topic HygonWLRaw --create \
   --replication-factor 1 --partitions 1
ll

$directory/bin/kafka-topics --zookeeper localhost:2181 \
   --topic HygonData --create \
   --replication-factor 1 --partitions 1

cd ../formatter
mvn install
