flink-bin:
	wget https://dlcdn.apache.org/flink/flink-1.15.0/flink-1.15.0-bin-scala_2.12.tgz
	tar -zxvf ./flink-1.15.0-bin-scala_2.12.tgz

kafka-bin:
	wget https://dlcdn.apache.org/kafka/3.2.0/kafka-3.2.0-src.tgz
	tar -zxvf ./kafka-3.2.0-src.tgz

start:
	cd flink-1.15.0 && ./bin/start-cluster.sh

open:
	open http://localhost:8081

run:
	#wget https://streaming-with-flink.github.io/examples/download/examples-scala.jar
	cd flink-1.15.0 && ./bin/flink run -c io.github.streamingwithflink.chapter1.AverageSensorReadings ../examples-scala.jar

stop:
	cd flink-1.15.0 && ./bin/stop-cluster.sh

reference-clone:
	gh repo clone immerok/recipes
	gh repo clone apache/flink-training
	gh repo clone ververica/flink-sql-cookbook
	gh repo clone wuchong/awesome-flink