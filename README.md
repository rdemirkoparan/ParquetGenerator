# ParquetGenerator
Generate Partitioned Parquet with Given Schema and Generated Value According to the Given Schema

The main purpose of this application is to develop a general solution for high performance writing in parallel to multiple parquet files, where partitions are based on a specific time-based interval. The application should be flexible to support any schema provided by the user.

## Usage

### Prerequisites

This part is only applicable for Windows users without local Hadoop installation!

* Please apply the solution https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-tips-and-tricks-running-spark-windows.html

As a short list of the link above
```
* download winutil from https://github.com/steveloughran/winutils/blob/master/hadoop-2.7.1/bin/winutils.exe
* create directory c:\hadoop\bin
* copy downloaded winutil.exe under c:\hadoop\bin
* set HADOOP_HOME=c:\hadoop
* set PATH=%HADOOP_HOME%\bin;%PATH%
* create directory C:\tmp\hive
* winutils.exe chmod -R 777 C:\tmp\hive
* winutils.exe ls -F C:\tmp\hive
```
* Path parameters should be compatible with Windows. Always start with '/' and all '\' characters must be escaped with '\'. Example output file parameter: "-o /%TEMP%\\par.out"

### Execution

```
Clone to your local repository: git clone https://github.com/rdemirkoparan/ParquetGenerator.git
Change directory: cd ParquetGenerator/
Compile: mvn compile
Run: mvn exec:java -Dexec.mainClass="ind.rd.parquet.PartitionedParquetGenerator" -Dexec.args="-i doc/sample.schema"
```

Full list of the parameters are listed below;

```
Input parameters;
 -i,--inputSchema <arg>            Input message schema, requires string as the full path of the schema file
 -l,--bufferLimit <arg>         	Number of records to buffer, accepts integer (10, 50, etc), default value is 200
 -m,--memoryLimit <arg>         	Maximum memory buffer while partitioning, requires string as usual memory parameter (1g, 2048m, etc), default value is 1g
 -o,--targetFileName <arg>      	Output file name, requires string as the full path of the partition directory, default value is /tmp/par.out
 -r,--numberOfRecords <arg>     	Number of records to generate, accepts integer (10, 9999, etc), default value is 1000
 -s,--partitionInterval <arg>   	Partitioning time interval as hours, accepts integer as hour (1, 12, etc), default value is 1
 -t,--threadCount <arg>         	Number of threads, accepts integer (2, 4, etc), default value is 4
 ```
 
### Running the tests

There is two test class exists.

* One for evolved to generate data, consume data and verify there is no record left behind
* The Second tester is to verify the partitions

```
mvn test
```

## Built With

* [Maven](https://maven.apache.org/) - Dependency Management

## Acknowledgments

* Use only for local testing

