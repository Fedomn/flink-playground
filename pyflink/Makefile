pyflink-libs:
	- $(eval LIB_PATH=$(shell pip3 show apache-flink | grep 'Location' | grep -oE ": (.*)" | cut -c3-))
	echo "${LIB_PATH}/pyflink/lib"
	wget https://github.com/knaufk/flink-faker/releases/download/v0.5.2/flink-faker-0.5.2.jar -O ${LIB_PATH}/pyflink/lib/flink-faker-0.5.2.jar
	wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.0/flink-sql-connector-kafka-1.17.0.jar -O ${LIB_PATH}/pyflink/lib/flink-sql-connector-kafka-1.17.0.jar
	wget https://repo1.maven.org/maven2/org/apache/flink/flink-parquet/1.17.0/flink-parquet-1.17.0.jar -O ${LIB_PATH}/pyflink/lib/flink-parquet-1.17.0.jar

pyflink-log:
	- $(eval LIB_PATH=$(shell pip3 show apache-flink | grep 'Location' | grep -oE ": (.*)" | cut -c3-))
	tail -f ${LIB_PATH}/pyflink/log/flink-frankma-python-*.log

pyflink-dir:
	- $(eval LIB_PATH=$(shell pip3 show apache-flink | grep 'Location' | grep -oE ": (.*)" | cut -c3-))
	cd ${LIB_PATH}/pyflink

clean:
	rm -rf sql/checkpoints
	rm -rf sql/offline_datawarehouse

shell:
	pyflink-shell.sh local
	# pyflink-shell.sh remote <hostname> <portnumber>

cluster-run:
	#https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/cli/
	#./bin/flink run --python examples/python/table/word_count.py