## We provide two solutions to use the WFC(Weber-Fechner Clustering) algorithm


### 1.The solution only with Apache Spark
- We used the [Apache Spark](https://spark.apache.org/docs/latest/index.html) framework to implement the entire process of the WFC algorithm, and you can easily run the WFC algorithm with a script.
Click [**here**](https://github.com/IoTDATALab/WFC/tree/master/spark/with_graphx) to learn how to use it.
- During the calculation of connected components in the WCF algorithm, the [***CC***](https://spark.apache.org/docs/latest/graphx-programming-guide.html#connected-components) (Connected Components) method in **Spark GraphX** did not show sufficient graph calculation performance. 
This solution is suitable for analyzing data with a small and medium amount in a single machine environment. 
It is especially suitable for verification of methods in the early stage.
If you want to do large-scale experiments with WFC, 
please use the second solution below.

### 2.The solution with both Apache Spark and GraphLab Create
- We used the Apache Spark framework and [GraphLab Create](https://turi.com/products/create/docs/index.html) machine learning tools to implement the WFC algorithm. 
Click [**here**](https://github.com/IoTDATALab/WFC/tree/master/spark/with_graphlab) to learn how to use it.
- We used the [***CC***](https://turi.com/products/create/docs/generated/graphlab.connected_components.create.html) (Connected Components) method provided by **GraphLab** instead of Spark GraphX in this solution. 
This can effectively improve the scalability of the entire WFC algorithm. 
This solution is suitable for analyzing large-scale data using WFC algorithm in a distributed cluster environment.