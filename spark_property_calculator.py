"""
Calculate EMR executor memory based on spark.executor.cores

- Ref
  - [AWS EMR blog](https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/)
  - [Spark properties doc](https://spark.apache.org/docs/latest/configuration.html)
"""


WORKER_NODE_COUNT = 2
CORES_PER_NODE, MEMORY_PER_NODE = 32, 128  # 8XL
# CORES_PER_NODE, MEMORY_PER_NODE = 48, 192  # 12XL
# CORES_PER_NODE, MEMORY_PER_NODE = 64, 256  # 16XL

CORES_PER_EXECUTOR = 4  # Default to 1 in YARN. AWS recommends 4 or 5.

total_executor_cores = CORES_PER_NODE - 1  # Reserve 1 core for Hadoop daemons
executors_per_node = int(total_executor_cores / CORES_PER_EXECUTOR)
total_memory_per_executor = MEMORY_PER_NODE / executors_per_node
# Total executor memory = executor memory + executor memory overhead (default
# to 10% of executor memory)

memory_per_executor = int(total_memory_per_executor / 1.1)  # Default to 1g
max_tasks_in_parallel = WORKER_NODE_COUNT * executors_per_node * CORES_PER_EXECUTOR

print(
    f'INFO: Worker node count: {WORKER_NODE_COUNT}',
    f'INFO: Worker node specs: {CORES_PER_NODE} cores, {MEMORY_PER_NODE} GiB RAM',
    f'INFO: Cores per executor: {CORES_PER_EXECUTOR}',
    '',
    f'Calculated Master node specs: {CORES_PER_EXECUTOR} cores, {memory_per_executor} GiB RAM',
    f'Calculated executors per node: {executors_per_node}',
    f'Calculated max tasks in parallel: {max_tasks_in_parallel}',
    sep='\n'
)

print(
    '\nspark-submit '
    '--deploy-mode cluster '
    f'--driver-memory {memory_per_executor}g '
    f'--executor-memory {memory_per_executor}g '
    f'--conf spark.driver.cores={CORES_PER_EXECUTOR} '
    f'--conf spark.executor.cores={CORES_PER_EXECUTOR} '
    '--conf spark.driver.maxResultSize=0 '
    '--conf spark.yarn.submit.waitAppCompletion=true '
)
