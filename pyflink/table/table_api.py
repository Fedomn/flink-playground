from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.table.expressions import col


def basic():
    """
    Flink 有两种关系型 API 来做流批统一处理：Table API 和 SQL。
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    table_env = StreamTableEnvironment.create(env)
    orders = table_env.from_elements(
        [('Jack', 'FRANCE', 10), ('Rose', 'ENGLAND', 30), ('Jack', 'FRANCE', 20)],
        ['name', 'country', 'revenue']
    )

    # +I 表示这是一条插入的消息
    # -U 表示这是一条撤回消息 (即更新前)，这意味着应该在 sink 中删除或撤回该消息
    # +U 表示这是一条更新的记录 (即更新后)，这意味着应该在 sink 中更新或插入该消息

    # table api
    revenue = orders \
        .select(col("name"), col("country"), col("revenue")) \
        .group_by(col("name")) \
        .select(col("name"), col("revenue").sum.alias('rev_sum'))
    revenue.execute().print()
    # explain plan tree
    print(revenue.explain())

    # sql: table to stream
    retract_stream = table_env.to_retract_stream(revenue, type_info=Types.TUPLE([Types.STRING(), Types.LONG()]))
    retract_stream.print()
    env.execute("table to stream")

    # sql: temp view
    table_env.create_temporary_view("orders", orders)
    table_result = table_env.execute_sql("select name, sum(revenue) as rev_sum from orders group by name")
    with table_result.collect() as results:
        for result in results:
            print(result)


if __name__ == '__main__':
    basic()
