# Massive Data Analysis

## How can we install spark on linux? 
Follw below instructions. 
1. Java
```bash
sudo apt install openjdk-8-jdk
java -version
sudo update-alternatives --config java
```
2. Python
```bash
sudo apt install python3.8
python --version
sudo update-alternatives --config python
```
3. Scala
```bash
sudo apt install scala
scala -version
```
4. Spark
```bash
wget https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
sudo tar xzvf spark-3.5.0-bin-hadoop3.tgz -C /opt/

nvim ~/.bashrc
export SPARK_HOME=/opt/spark-3.5.0-bin-hadoop3
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin:~/.local/bin
export PYTHONPATH=$(ZIPS=(""/python/lib/*.zip); IFS=:; echo "${ZIPS[*]}"):$PYTHONPATH

source ~/.bashrc
```

## Run a simple spark program
```python
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

sc.parallelize([1, 2]).collect()
```
