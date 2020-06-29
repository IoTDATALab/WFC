# Weber-Fechner Clustering (WFC) implementation based on Apache Spark and GraphLab Create
This project is the Weber-Fechner Clustering algorithm implementation based on **Apache Spark(Scala)** and **GraphLab Create(Python)**. 
   
This framework mainly construct with scala codes, only 'Connected Components' part is based on
GraphLab in Python, so this implementation runs with bash scripts. 

The solution only based on Spark is [**here**](https://github.com/IoTDATALab/WFC/tree/master/spark/with_graphx).

## 1.Program Input
### 1.1 data set
  * Data set of **txt** or **csv** file is input by given directory -- "*DATA_PATH*" in bash script "**parameters.sh**".
  * Each item in data set need to be separated by "**,**" or other white delimiters like **spaces** or **tabs**.  
Example:
  ```bash
  # cluster model
  export DATA_PATH="hdfs/path/to/data"
  # stand alone model
  export DATA_PATH="file/path/to/data"
  ```  
### 1.2 lambda
  * The parameter lambda is the Weber-Fechner coefficient, and it is bigger than 0.
  * In MinHash coding model, you are required to give this parameter. It dose not work in SD coding model.  
Example:
  ```bash
  export LAMBDA=0.02
  ```
### 1.3 Other optional parameters
  * You can set them in bash script "**parameters.sh**"  
Example:
  ```bash
  # Optional parameters
  # 1. START_SCALE: Setting encoding starting scale
  # 2. END_SCALE: Setting encoding ending scale
  # 3. CODE_MODEL: Setting encoding model. Setting SD for SD code, MIN for MinHash Code
  # 4. DATA_STANDARDIZATION: Standardized input data or not
  # 5. NUMBER_OF_HASHTABLES and NUMBER_OF_BUCKETS: Setting MinHashLSH parameters
  export START_SCALE=1
  export END_SCALE=2
  export CODE_MODEL=SD
  export DATA_STANDARDIZATION=TRUE
  export NUMBER_OF_HASHTABLES=125
  export NUMBER_OF_BUCKETS=25
  ```
  
## 2. Program Output
### 2.1 Clustering results
  * For storing temporary generated data and cluster results, 
    you need to give a directory -- "*OUTPUT_PATH*" in bash script "**parameters.sh**".
  * You can get clustering results in different scale in algorithm.
  * The results are pairs of item ids and label -- **"id,label"**.
Example:
  ```bash
  # cluster model
  export OUTPUT_PATH="hdfs/path/to/save/results"
  # stand alone model
  export OUTPUT_PATH="file/path/to/save/results"
  ```

## 3. Environment Deployment
### 3.1 Requirements
 * [HDFS(hadoop-2.7.4)](https://hadoop.apache.org/docs/r2.7.5/)
 * [spark-2.1.2-bin-hadoop2.7](http://spark.apache.org/downloads.html)
 * [GraphLab Create v2.1](https://turi.com/download/install-graphlab-create.html)  
**Note**: python version need to be *python 2.7.X*
### 3.2 Starting Spark before using WFC
 * HDFS should be started before you run this project if you run WFC on cluster environment. Run this command:     
   
  ```
  start-dfs.sh
  ```
 * Spark cluster should be started before you run this project. Run these commands:   
  ```
  start-master.sh
  start-slaves.sh
  ```
 * Stop them after running if you do not use them any more. Run these commands:  
  ```
  stop-slaves.sh   
  stop-master.sh   
  stop-dfs.sh
  ``` 
 
## 4. Example of clustering with WFC on cluster environment
### Step 1: Modify the runtime environment file "**environment.sh**" according to you hardware.
 * These parameter are set depending on you computing resource.
 * You can learn how to optimized them in 
 [Spark Tuning](https://spark.apache.org/docs/latest/tuning.html) and 
 [Spark Configuration](https://spark.apache.org/docs/latest/configuration.html)

### Step 2: Modify your input parameters file "**parameters.sh**"
### Step 3: Package the program to "**WFC-GraphLab.jar**", and put it into folder "**scriptRunner**"
### Step 4: Copy the folder "**scriptRunner**" to the client node, and start clustering
 * In cluster model, client node can be a master node or a slaver node.  
 * Then you just need to run the bash script "**stepsExecutor.sh**" in folder "**scriptRunner**" with commands:
  ```bash
 cd ~/scriptRunner
 bash stepsExecutor.sh
 ```
 
## 5. Suggestions about setting optional parameters
### 5.1 CODE_MODEL
There are two encoding models that you can choose which model to use, SD code model and For MinHasH code model  
We suggest that you set it SD if the data dimensions id less than 4, otherwise set it MIN.  
Example:
```bash
  # Encoding with SD code if dimensions is less than 4.
  export CODE_MODEL=SD
  # Encoding with MIN code if dimensions is 4 or larger than 4
  export CODE_MODEL=MIN
```
### 5.2 MinHashLSH parameters -- NUMBER_OF_HASHTABLES and NUMBER_OF_BUCKETS
There two parameters for MinHash coding model.
Parameter *NUMBER_OF_HASHTABLES* is set *125*, and '*NUMBER_OF_BUCKETS*' is set *25* by default:
* *NUMBER_OF_HASHTABLES* , using to set MinHash signature length.
* *NUMBER_OF_BUCKETS* , using to set LSH buckets number.

**Notice that**:  
(1) Parameter *NUMBER_OF_HASHTABLES* must be **divisible** by '*NUMBER_OF_BUCKETS*'.  
(2) *rows = NUMBER_OF_HASHTABLES / NUMBER_OF_BUCKETS*. The smaller '*rows*' is, the more accurate clustering is, but the lower 
    program execution efficiency is, and more likely you will get **IO exception of stack overflow**. To avoid this
    problem, another way to maintain or improve the accuracy of clustering is to increase '*NUMBER_OF_HASHTABLES*', and 
    adjust '*NUMBER_OF_BUCKETS*' at the same time to keep an appropriate '*rows*'.  
Example:
```bash
# row is 5 by default, and now it is 10
export NUMBER_OF_HASHTABLES=180
export NUMBER_OF_BUCKETS=18
```

### 5.3 Weber scale range -- START_SCALE and END_SCALE
The two parameters decide the encoding scale range from *START_SCALE* to *END_SCALE*.
* *START_SCALE* is set *1* by default. 
* *END_SCALE* is set *0* by default, and we can calculate the *max coding scale* if you do not give them.
* If you do not set *END_SCALE* with 0, *END_SCALE* must be set bigger than *START_SCALE*.  
Example:
```bash
# You can do that to get clustering results in scale 3~9.
export START_SCALE=3
export END_SCALE=9

# You can do that if you want to get a single scale(9 for example) clustering result .
export START_SCALE=9
export END_SCALE=9
```

## 6. Reference
Waiting for modification
