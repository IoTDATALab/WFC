# Weber-Fechner Clustering (WFC) implementation based on Spark GraphX
This project is a implementation of the WFC algorithm based on **Spark GraphX** in Scala. 

The solution based on GraphLab Create is [**here**](https://github.com/IoTDATALab/WFC/tree/master/spark/with_graphlab).



## 1. Program Input
### 1.1 data set
  * Each item in data set need to be separated by "**,**" or other white delimiters like **spaces** or **tabs**.
  ```scala
  //clustering with distributed files
  val dataPath="hdfs/path/to/data"
  ```
  ```scala
  //clustering with local files
  val dataPath="local/path/to/data"
  ```
    
### 1.2 lambda
  * The parameter lambda is the Weber-Fechner coefficient, and it is bigger than 0.
  * Actually it is *1* in SD coding model.


### 1.3 Other optional parameters
  * You can set them before you call "*WFC.cluster()*"
  * Example:
  ```scala
  import com.iotdatalab.cluster.algorithm.WFC
  
   /**
   * Optional parameters and settings:
   *     1. startScale: Setting encoding starting scale
   *     2. endScale: Setting encoding ending scale
   *     3. codeModel: Setting encoding model. Setting SD for SD code, MIN for MinHash Code
   *     4. standardization: Standardized input data or not
   *     5. numHashTables and numBuckets: Setting MinHashLSH parameters
   */
    WFC.setMinHashLSHParas(numHashTables=180,numBuckets=18)
    WFC.setScaleRange(startScale=1,endScale=10)
    WFC.setEncodingModel(codeModel="SD")
    WFC.setDataStandardization(standardization=true)
  ```
  
## 2. Program Output
### 2.1 Clustering results
  * For storing temporary generated data and cluster results, 
    you need to give a directory "outputPath"
  * You can get clustering results in different scale in algorithm.
  * The results are pairs of item ids and label -- **"id,label"**.

## 3. Environment Deployment
### 3.1 Requirements
 * [HDFS(hadoop-2.7.4)](https://hadoop.apache.org/docs/r2.7.5/)
 * [spark-2.1.2-bin-hadoop2.7](http://spark.apache.org/downloads.html)

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
 
## 4. Example of starting clustering with WFC
  ```scala
   import com.iotdatalab.cluster.algorithm.WFC
   import org.apache.spark.rdd.RDD
   import org.apache.spark.{SparkConf, SparkContext}
   
   object WFC_Example {
   
     private val defaultNumPartition = sys.env.get("SPARK_DEFAULT_PARALLELISM").get.toInt
   
     def main(args: Array[String]) {
   
       val lambda: Double = 0.02
       val dataPath = "/path/to/data/data.csv"
       val outputPath = "/path/to/save/result"
   
       val destOut = outputPath.split('/').last
       val conf = new SparkConf()
         .setAppName(s"Weber-Fechner clustering(lambda=$lambda) -> $destOut")
         .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
         .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")

       val sc = new SparkContext(conf)
       //Optional Settings
       WFC.setMinHashLSHParas(numHashTables=180,numBuckets=18)
       WFC.setScaleRange(startScale=1,endScale=10)
       WFC.setEncodingModel(codeModel="SD")
       WFC.setDataStandardization(standardization=true)
   
       //Clustering and outputting results
       val data: RDD[String] = sc.textFile(dataPath, defaultNumPartition)
       WFC.cluster(data, lambda, outputPath)
       sc.stop()
     }
   }
  ```
## 5. Example of clustering with WFC on cluster environment
### Step 1: Modify the runtime environment file "**environment.sh**" according to you hardware.
 * These parameter are set depending on you computing resource.
 * You can learn how to optimized them in 
 [Spark Tuning](https://spark.apache.org/docs/latest/tuning.html) and 
 [Spark Configuration](https://spark.apache.org/docs/latest/configuration.html)

### Step 2: Package the program to "**WFC-Spark.jar**", and put it into folder "**scriptRunner**"
### Step 3: Copy the folder "**scriptRunner**" to the client node, and start clustering
 * In cluster model, client node can be a master node or a slaver node.  
 * Then you just need to run the bash script "**submit.sh**" in folder "**scriptRunner**" with commands:
  ```bash
 cd ~/scriptRunner
 bash submit.sh
 ```
 
## 6. Suggestions about setting optional parameters
### 6.1 codeModel
There are two encoding models -- SD code model and MinHasH code model, and you can choose either of two to use.  
We suggest that you set it "SD" if the data dimensions id less than 4, otherwise set it "MIN".

### 6.2 MinHashLSH parameters -- numHashTables and numBuckets
There two parameters for MinHash coding model.
Parameter *numHashTables* is set *125*, and '*numBuckets*' is set *25* by default:
* *numHashTables* , using to set MinHash signature length.
* *numBuckets* , using to set LSH buckets number.

**Notice that**:  
(1) Parameter *numHashTables* must be **divisible** by '*numBuckets*'.  
(2) *rows = numHashTables / numBuckets*. The smaller '*rows*' is, the more accurate clustering is, but the lower 
    program execution efficiency is, and more likely you will get **IO exception of stack overflow**. To avoid this
    problem, another way to maintain or improve the accuracy of clustering is to increase '*numHashTables*', and 
    adjust '*numBuckets*' at the same time to keep an appropriate '*rows*'.


### 6.3 Weber scale range -- startScale and endScale
The two parameters decide the encoding scale range from *startScale* to *endScale*.
* *startScale* is set *1* by default. 
* *endScale* is set *0* by default, and we can calculate the *max coding scale* if you do not give them.
* If you do not set *endScale* with 0, *endScale* must be set bigger than *startScale*.
* You can set *startScale* and *endScale* with the same value if you want to get a single scale clustering result.

## 7. Reference
Waiting for modification
