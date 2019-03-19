# ParquetGenerator
Generate Partitioned Parquet with Given Schema and Generated Value According to the Given Schema

The main purpose of this application is to develop a general solution for high performance writing in parallel to multiple parquet files, where partitions are based on a specific time-based interval. The application should be flexible as to support any schema provided by the user.

## Usage

```
Clone to your local repository
Compile: mvn compile
Run: mvn exec:java -Dexec.mainClass="ind.rd.parquet.PartitionedParquetGenerator" -Dexec.args="-i /tmp/sample.schema"
```

```
Input parameters;
 -i,--inputSchema <arg>        	Input message schema
 -l,--bufferLimit <arg>         		Number of records to buffer
 -m,--memoryLimit <arg>         	Maximum memory buffer while partitioning
 -o,--targetFileName <arg>      	Output file name
 -r,--numberOfRecords <arg>     	Number of records to generate
 -s,--partitionInterval <arg>   	Partitioning time interval as hours
 -t,--threadCount <arg>         	Number of threads
 ```
 
### Running the tests

Coming soon..

## Built With

* [Maven](https://maven.apache.org/) - Dependency Management

## Acknowledgments

* Use only for local testing

