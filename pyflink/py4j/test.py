from pyflink.datastream import StreamExecutionEnvironment

if __name__ == '__main__':
    from pyflink.java_gateway import get_gateway

    gw = get_gateway()
    _j_env = StreamExecutionEnvironment.get_execution_environment()._j_stream_execution_environment
    read_cdc = gw.jvm.org.playground.flink.datastream.CDC.test_py4j(_j_env)
