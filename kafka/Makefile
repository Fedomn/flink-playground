kafka-bin:
	wget https://dlcdn.apache.org/kafka/3.4.0/kafka-3.4.0-src.tgz
	tar -zxvf ./kafka-3.4.0-src.tgz
	mv kafka-3.4.0-src kafka-src

init:
	$(eval uuid=$(shell cd ./kafka-src && ./bin/kafka-storage.sh random-uuid))
	cd ./kafka-src && ./bin/kafka-storage.sh format -t ${uuid} -c ./config/kraft/server.properties

start:
	cd ./kafka-src && ./bin/kafka-server-start.sh ./config/kraft/server.properties

create-topic:
	cd ./kafka-src && \
	./bin/kafka-topics.sh --create --topic browser-status-codes --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092

show-topic:
	cd ./kafka-src && \
	./bin/kafka-topics.sh --describe --topic browser-status-codes --bootstrap-server localhost:9092

producer:
	cd ./kafka-src && \
	bin/kafka-console-producer.sh --topic foo --bootstrap-server localhost:9092

consumer:
	cd ./kafka-src && \
	./bin/kafka-console-consumer.sh --topic browser-status-codes --from-beginning --bootstrap-server localhost:9092

clean:
	rm -rf /tmp/kraft-combined-logs

kafka-dump-log:
	cd ./kafka-src && \
	./bin/kafka-dump-log.sh  --cluster-metadata-decoder --skip-record-metadata --files /tmp/kraft-combined-logs/__cluster_metadata-0/*.log

kafka-metadata-shell:
	cd ./kafka-src && \
	./bin/kafka-metadata-shell.sh  --snapshot /tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log

# kafka connect debezium
# requirements: https://debezium.io/documentation/reference/2.3/connectors/mysql.html#mysql-deploying-a-connector
start-connector:
	cd ./kafka-src && \
	./bin/connect-standalone.sh ./config/connect-standalone.properties

connector-api-doc:
	open https://kafka.apache.org/documentation/#connect_rest
	curl http://localhost:8088/connectors