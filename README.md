# INDE2

Téléchargez la dernière version d'Apache Kafka à partir du site officiel : https://downloads.apache.org/kafka/3.5.0/kafka_2.13-3.5.0.tgz
tar -xzf kafka_2.13-3.5.0.tgz && cd kafka_2.13-3.5.0
bin/zookeeper-server-start.sh config/zookeeper.properties
Dans un autre terminal et dans la racine du dossier :
bin/kafka-server-start.sh config/server.properties
curl -s "https://get.sdkman.io" | bash
Dans un nouveau terminal :
sdk install scala 2.13.11
sdk install sbt
cd inde2
sbt compile
sbt run
