flink-bin:
	wget https://dlcdn.apache.org/flink/flink-1.16.1/flink-1.16.1-bin-scala_2.12.tgz
	tar -zxvf ./flink-1.16.1-bin-scala_2.12.tgz

start:
	cd flink-1.16.1 && ./bin/start-cluster.sh

open:
	open http://localhost:8081

run:
	#wget https://streaming-with-flink.github.io/examples/download/examples-scala.jar
	cd flink-1.16.1 && ./bin/flink run -c io.github.streamingwithflink.chapter1.AverageSensorReadings ../examples-scala.jar

stop:
	cd flink-1.16.1 && ./bin/stop-cluster.sh

reference-clone:
	gh repo clone immerok/recipes
	gh repo clone apache/flink-training
	gh repo clone ververica/flink-sql-cookbook
	gh repo clone wuchong/awesome-flink

pyflink-libs:
	- $(eval LIB_PATH=$(shell pip3 show apache-flink | grep 'Location' | grep -oE ": (.*)" | cut -c3-))
	echo "${LIB_PATH}/pyflink/lib"
	wget https://github.com/knaufk/flink-faker/releases/download/v0.5.2/flink-faker-0.5.2.jar -O ${LIB_PATH}/pyflink/lib/flink-faker-0.5.2.jar
	wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.16.0/flink-sql-connector-kafka-1.16.0.jar -O ${LIB_PATH}/pyflink/lib/flink-sql-connector-kafka-1.16.0.jar
	wget https://repo1.maven.org/maven2/org/apache/flink/flink-parquet/1.16.1/flink-parquet-1.16.1.jar -O ${LIB_PATH}/pyflink/lib/flink-parquet-1.16.1.jar

pyflink-prepare:
	 python3 -m pip install apache-flink
