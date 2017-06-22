# Batch process

In this project batch process is using once for historical data.

## Difference between approaches

* **spark_batch_map.py** works with mapping each rdd and processes all tasks(check users, check friendsship, write transaction) together.  
* **spark_batch_mapPartitions.py** works with partitions and at the beginning processes check friends, check friendship and after write transactions as sub- batch process
