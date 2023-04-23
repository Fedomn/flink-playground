flink-bin:
	wget https://dlcdn.apache.org/flink/flink-1.17.0/flink-1.17.0-bin-scala_2.12.tgz
	tar -zxvf ./flink-1.17.0-bin-scala_2.12.tgz

start:
	cd flink-1.17.0 && ./bin/start-cluster.sh
	open http://localhost:8081

run:
	#wget https://streaming-with-flink.github.io/examples/download/examples-scala.jar
	cd flink-1.17.0 && ./bin/flink run -c io.github.streamingwithflink.chapter1.AverageSensorReadings ../examples-scala.jar

stop:
	cd flink-1.17.0 && ./bin/stop-cluster.sh

reference-clone:
	gh repo clone immerok/recipes
	gh repo clone apache/flink-training
	gh repo clone ververica/flink-sql-cookbook
	gh repo clone wuchong/awesome-flink
