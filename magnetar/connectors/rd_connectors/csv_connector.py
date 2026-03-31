"""
Simple CSV connector for Magnetar. It uses Spark's built-in CSV reader. 
The idea is to make the read really fast by using Spark's optimizations, and to allow for easy integration with Spark-based workflows.
"""

from magnetar.connectors.base_connector import BaseConnector 
from typing import Iterator, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark import RDD
from functools import wraps
import logging 
import time 

logger = logging.getLogger(__name__)

# Decorator to ensure connector is connected. 
# Learning about decorators and wrappers ! 
def require_connection(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self.is_connected:
            logger.error(f"CONNECTION ERROR : CSVConnector {self.connector_id} is not connected. Please call connect() before performing this operation.")
            raise ConnectionError(f"CSVConnector {self.connector_id} is not connected. Please call connect() before performing this operation.")
        return func(self, *args, **kwargs)
    return wrapper

def safe_call(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            if func.__name__ == "write":
                self.statistics["write_errors"] += 1
            elif func.__name__ == "read":
                self.statistics["read_errors"] += 1
            logger.error(f"ERROR in {func.__name__} : CSVConnector {self.connector_id} encountered an error: {str(e)}")
            raise RuntimeError(f"CSVConnector {self.connector_id} encountered an error in {func.__name__}: {str(e)}")
    return wrapper

class CSVConnector(BaseConnector):
    CONNECTOR_TYPE : str = "csv"
    LOGGER = logging.getLogger(__name__)
    
    def __init__(self, connect_auto = True, **kwargs):
        """
        Parameters:
            path (str): The file path to the CSV file. This can be a local path or a path in a distributed file system like HDFS or S3.
            spark_session (SparkSession): A Spark session object. This is required for reading the CSV file using Spark.
            delimiter (str, optional): The delimiter used in the CSV file. Default is ','.
            header (bool, optional): Whether the CSV file has a header row. Default is True.
            infer_schema_flag (bool, optional): Whether to let Spark infer the schema of the CSV
            schema (StructType, optional): A Spark StructType object defining the schema of the CSV file. If provided, this will be used instead of inferring the schema.
            encoding (str, optional): The encoding of the CSV file. Default is 'utf-8'.
            multiline (bool, optional): Whether the CSV file contains multiline fields. Default is False.
            null_value (str, optional): A string that represents null values in the CSV file. Default is None.
            nan_value (str, optional): A string that represents NaN values in the CSV file. Default is "NaN".
            date_format (str, optional): The date format used in the CSV file. Default is "yyyy-MM-dd".
            timestamp_format (str, optional): The timestamp format used in the CSV file. Default is "yyyy-MM-dd'T'HH:mm:ss".
            max_columns (int, optional): The maximum number of columns to read from the CSV file. Default is 40960.
            read_options (dict, optional): Additional options to pass to Spark's CSV reader. Default is an empty dictionary.
            write_options (dict, optional): Additional options to pass to Spark's CSV writer. Default is an empty dictionary.
            write_mode (str, optional): The write mode to use when writing CSV files with Spark. Default is "overwrite". Can be "overwrite", "append", "ignore", or "error" (default).
            write_partitions (list[str], optional): A list of column names to partition the output CSV by when writing with Spark. Default is None (no partitioning).
            sample_fraction (float, optional): The fraction of the CSV file to read for sampling purposes. Default is 1.0 (read the entire file).
        """
        super().__init__(connector_type=self.CONNECTOR_TYPE, **kwargs)
        # -- required parameters --
        self.path: str = kwargs["path"]  # Required parameter for CSV file path
        self.spark_session: SparkSession = kwargs["spark_session"] # Required Spark session for reading CSV with Spark
        # -- optional parameters --
        self.delimiter: str = kwargs.get("delimiter", ",")  # Optional parameter for CSV delimiter, default is comma
        self.header: bool = kwargs.get("header", True)  # Optional parameter to indicate if the CSV has a header row, default is True
        self.infer_schema_flag: bool = kwargs.get("infer_schema_flag", False)  # Optional parameter to indicate if Spark should infer the schema, default is True
        self.schema: Optional[StructType] = kwargs.get("schema", None)  # Optional parameter for providing a Spark schema, default is None
        self.encoding: str = kwargs.get("encoding", "utf-8")  # Optional parameter for file encoding, default is utf-8
        self.multiline: bool = kwargs.get("multiline", False)  # Optional parameter to indicate if the CSV contains multiline fields, default is False
        self.null_value: str = kwargs.get("null_value", None)  # Optional parameter to specify a string that represents null values in the CSV, default is None
        self.nan_value: str= kwargs.get("nan_value", "NaN")  # Optional parameter to specify a string that represents NaN values in the CSV, default is "NaN"
        self.date_format: str = kwargs.get("date_format", "yyyy-MM-dd")  # Optional parameter to specify the date format in the CSV, default is "yyyy-MM-dd"
        self.timestamp_format: str = kwargs.get("timestamp_format", "yyyy-MM-dd'T'HH:mm:ss")  # Optional parameter to specify the timestamp format in the CSV, default is "yyyy-MM-dd HH:mm:ss"
        self.max_columns: int = kwargs.get("max_columns", 40960)  # Optional parameter to specify the maximum number of columns to read from the CSV, default is 1000
        self.read_options : dict = kwargs.get("read_options", {})  # Optional parameter to specify additional options for Spark's CSV reader, default is an empty dictionary
        self.write_options : dict = kwargs.get("write_options", {})  # Optional parameter to specify additional options for Spark's CSV writer, default is an empty dictionary
        self.write_mode: str = kwargs.get("write_mode", "overwrite")  # Optional parameter to specify the write mode for Spark's CSV writer, default is "overwrite". Can be "overwrite", "append", "ignore", or "error" (default)
        self.write_partitions : list[str]|None = kwargs.get("write_partitions", None)  # Optional parameter to specify the columns to partition the output CSV by when writing with Spark, default is None (no partitioning)
        self.sample_fraction: float = kwargs.get("sample_fraction", 1.0)  # Optional parameter to specify the fraction of the CSV file to read for sampling purposes, default is 1.0 (read the entire file)
        self.is_connected: bool = False  # Flag to indicate if the connector is connected to the data source, default is False
        self.spark_df: Optional[DataFrame] = None  # Cache for the Spark DataFrame read from the CSV file, default is None
        self.statistics: dict = {
            "reads": 0,  # Counter for the number of times the CSV file has been read
            "writes": 0,  # Counter for the number of times data has been written to a CSV file
            "rows_read": 0,  # Counter for the total number of rows read from the CSV file
            "rows_written": 0,  # Counter for the total number of rows written to
            "read_errors": 0,  # Counter for the number of errors encountered while reading the CSV file
            "write_errors": 0,  # Counter for the number of errors encountered while writing
            "total_read_ms": 0,  # Total time in milliseconds spent reading the CSV file 
            "total_write_ms": 0,  # Total time in milliseconds spent writing to a CSV file
        }  # Dictionary to store statistics about the CSV file, such as number of rows, columns, etc., default is an empty dictionary
        if connect_auto:
            self.connect()  # Automatically connect to the data source upon initialization if connect_auto is True, which can simplify usage in cases where an immediate connection is desired.

    @property # function that turns into variable 
    def spark(self) -> SparkSession: # Lazy initialization of the Spark session. self.spark = self.spark_session actually .
        if not self.spark_session:
            self.spark_session = SparkSession.builder.appName(f"Magnetar-CSVConnector-{self.connector_id}").getOrCreate()
        return self.spark_session

    def __base_reader(self): # The main reader from pyspark. 
        """
        Return a pre-configured dataframe reader. 
        """
        reader = (
            self.spark.read
            .format("csv")
            .option("delimiter", self.delimiter)
            .option("header", str(self.header).lower())
            .option("encoding", self.encoding)
            .option("multiLine", str(self.multiline).lower())
            .option("nullValue", self.null_value)
            .option("nanValue", self.nan_value)
            .option("dateFormat", self.date_format)
            .option("timestampFormat", self.timestamp_format)
            .option("maxColumns", self.max_columns)
            .option("mode", "PERMISSIVE")# Set the mode to PERMISSIVE to allow Spark to handle malformed rows gracefully, instead of throwing an error. This will help improve the robustness of the CSV reading process, especially when dealing with large and potentially messy CSV files.
            .option("columnNameOfCorruptRecord", "_corrupt_record") # Add a column to capture any corrupt records that cannot be parsed correctly, which can help with debugging and data quality checks.
        )
        if self.schema: 
            reader = reader.schema(self.schema)
        elif self.infer_schema_flag: 
            reader = reader.option("inferSchema", "true")
        if self.read_options:
            reader = reader.options(**self.read_options)

        if self.read_options: 
            reader = reader.options(**self.read_options)
        return reader
    
    def __base_writer(self, df: DataFrame, **kwargs) -> DataFrame: # The main writer from pyspark.
        """
        Return a pre-configured dataframe writer. 
        """
        writer = (
            df.write
            .format("csv")
            .option("delimiter", self.delimiter)
            .option("header", str(self.header).lower())
            .option("encoding", self.encoding)
            .option("multiLine", str(self.multiline).lower())
            .option("nullValue", self.null_value)
            .option("nanValue", self.nan_value)
            .option("dateFormat", self.date_format)
            .option("timestampFormat", self.timestamp_format)
            .mode(self.write_mode) # Use the specified write mode for handling existing files or partitions when writing the CSV file, which can help prevent accidental data loss or duplication.
        )
        if self.write_partitions:
            writer = writer.partitionBy(*self.write_partitions)
        if self.write_options:
            writer = writer.options(**self.write_options)
        if kwargs:
            writer = writer.options(**kwargs)
        return writer
    
    def connect(self) -> None: 
        """
        Verify the SparkSession is live and mark the connector as ready. 
        """
        foo = self.spark
        self.is_connected = True
        logger.info(f"CONNECTED : CSVConnector {self.connector_id} connected to path: {self.path}")

    def disconnect(self) -> None: 
        """
        Mark the connector as disconnected. We won't actually stop the Spark session here, since it may be shared across multiple connectors and workflows. 
        """
        self.spark_df = None # Clear the cached Spark DataFrame to free up memory   
        self.is_connected = False
        logger.info(f"DISCONNECTED : CSVConnector {self.connector_id} disconnected from path: {self.path}")

    def test_connection(self):
        """
        Ping the source by reading just the header row.
        Returns True if successful, error message if not.
        """
        try:  
            ( # The spark reader will load only the header and limit 1 then counts the rows. 
                self.__base_reader()
                .option("header","true")
                .load(self.path)
                .limit(1)
                .count()
            )
            logger.info(f"TEST CONNECTION SUCCESS : CSVConnector {self.connector_id} successfully read header from path: {self.path}")
            return True
        except Exception as e:
            logger.error(f"TEST CONNECTION FAILED : CSVConnector {self.connector_id} failed to read header from path: {self.path}. Error: {str(e)}")
            raise ConnectionError(f"CSVConnector {self.connector_id} failed to read header from path: {self.path}. Error: {str(e)}")
    
    def validate_config(self):
        """
        If anything is wrong with the configuration, raises error
        """
        if not self.path: 
            logger.error(f"CONFIGURATION ERROR : CSVConnector {self.connector_id} configuration error: 'path' is required.")
            raise ValueError(f"CSVConnector {self.connector_id} configuration error: 'path' is required.")
        valid_modes = {"overwrite", "append", "ignore", "error", "errorifexists"}
        if self.write_mode not in valid_modes:
            logger.error(f"CONFIGURATION ERROR : CSVConnector {self.connector_id} configuration error: 'write_mode' must be one of {valid_modes}.")
            raise ValueError(f"CSVConnector {self.connector_id} configuration error: 'write_mode' must be one of {valid_modes}.")
        if not isinstance(self.sample_fraction, (int, float)) or not (0 < self.sample_fraction <= 1):
            logger.error(f"CONFIGURATION ERROR : CSVConnector {self.connector_id} configuration error: 'sample_fraction' must be a number between 0 and 1 (0,1].")
            raise ValueError(f"CSVConnector {self.connector_id} configuration error: 'sample_fraction' must be a number between 0 and 1 (0,1].")
        logger.info(f"CONFIGURATION VALIDATED : CSVConnector {self.connector_id} configuration validated successfully.")

    # Core I/O methods
    @safe_call
    @require_connection
    def read(self, cache:bool = True, **kwargs) -> DataFrame: 
        """
        Loading the CSV file into a Spark Dataframe
        cache (bool) : Whether to cache the resulting df 
        """ 
        T0 = time.monotonic()
        reader = self.__base_reader()
        if kwargs:
            reader = reader.options(**kwargs)
        self.spark_df = reader.load(self.path)
        if cache:  
            self.spark_df = self.spark_df.cache() # Cache the DataFrame in memory for faster subsequent reads.

        # Update statistics
        elapsed = (time.monotonic() - T0) * 1000  # Convert to milliseconds
        row_count = self.spark_df.count()  # Trigger an action to get the number of rows, which will also help populate the statistics about the read operation.
        self.statistics["reads"] += 1
        self.statistics["rows_read"] += row_count
        self.statistics["total_read_ms"] += elapsed
        logger.info(f"READ SUCCESS : CSVConnector {self.connector_id} read {row_count} rows from path: {self.path} in {elapsed:.2f} ms.")
        self.log_read_event()
        return self.spark_df
    
    @safe_call
    @require_connection
    def write(self, df: DataFrame, destination: str, **kwargs) -> None:
        """
        Persist a Spark DataFrame to a CSV file. 
        """
        T0 = time.monotonic() 
        row_count = df.count()  # Get the number of rows in the DataFrame to be written, which will help populate the statistics about the write operation.
        writer = self.__base_writer(df, **kwargs)
        writer.save(destination)
        elapsed = (time.monotonic() - T0) * 1000  # Convert to milliseconds
        self.statistics["writes"] += 1
        self.statistics["rows_written"] += row_count
        self.statistics["total_write_ms"] += elapsed
        logger.info(f"WRITE SUCCESS : CSVConnector {self.connector_id} wrote {row_count} rows to destination: {destination} in {elapsed:.2f} ms.")

    @safe_call
    @require_connection
    def yield_data(self, batch_size: int = 10000, **kwargs) -> Iterator[DataFrame]:
        """
        Yield data in batches from the csv
        """
        if self.spark_df is None:
            self.read(**kwargs)  # Load the DataFrame if it hasn't been loaded yet
        total = self.spark_df.count()  # Get the total number of rows in the DataFrame to be yielded, which will help with logging and statistics about the yield operation.
        num_parts = max(1, total // batch_size)  # Calculate the number of partitions needed based on the total number of rows and the specified batch size, ensuring at least one partition.
        __batches = self.spark_df.repartition(num_parts)  # Repartition the DataFrame to the calculated number of partitions to optimize the batch yielding process.
        for idx in range(num_parts):
            batch = __batches.rdd.mapPartitionsWithIndex(
                lambda ix, it: it if ix == idx else iter([])
            ).toDF(self.spark_df.schema)  # Convert the RDD back to a DataFrame with the original schema for each batch.
            yield batch  # Yield the current batch as a DataFrame for processing in the calling code.

    @safe_call
    @require_connection
    def read_schema(self, **kwargs) -> StructType:
        """
        Read just the schema of the CSV file without loading the data. This can be useful for understanding the structure of the data and for validating the configuration before performing a full read.
        """
        sample_df = (
            self.__base_reader()
            .option("inferSchema", "true")  # Ensure that Spark infers the schema when reading the sample, which will allow us to return an accurate StructType representing the structure of the CSV file.
            .load(self.path)
            .limit(1)  # Read a small sample of the data to infer the schema
        )
        return sample_df.schema
    
    # Spark integration methods
    @safe_call
    @require_connection
    def to_spark(self, spark_session: Optional[SparkSession]= None, **kwargs) -> DataFrame:
        """
        Return the CSV as a Spark DataFrame.
        """
        if spark_session: 
            self.spark_session = spark_session
        return self.spark_df if self.spark_df is not None else self.read(**kwargs)
    
    @safe_call
    @require_connection
    def to_rdd(self, spark_session: Optional[SparkSession]= None, **kwargs) -> RDD:
        """
        Return the underlying RDD of Row objections.
        """
        return self.to_spark(spark_session=spark_session, **kwargs).rdd
    
    @safe_call
    @require_connection
    def get_spark_schema(self, **kwargs) -> StructType:
        """
        Return the Spark schema of the CSV file as a StructType object. This can be useful for understanding the structure of the data and for validating the configuration before performing a full read.
        """
        return self.read_schema(**kwargs)
    
    @safe_call
    @require_connection
    def read_partitioned(self, partition_col : str, num_partitions:int, **kwargs) -> DataFrame:
        """
        Read by repartitioning by a column
        """
        if self.spark_df is None:
            self.read(**kwargs)  # Load the DataFrame if it hasn't been loaded yet
        if partition_col not in self.spark_df.columns:
            logger.error(f"PARTITIONING ERROR : CSVConnector {self.connector_id} partitioning error: specified partition column '{partition_col}' does not exist in the DataFrame.")
            raise ValueError(f"CSVConnector {self.connector_id} partitioning error: specified partition column '{partition_col}' does not exist in the DataFrame.")
        return self.spark_df.repartition(num_partitions, F.col(partition_col))
    
    @safe_call
    @require_connection
    def get_metadata(self, **kwargs) -> dict:
        if self.spark_df is None:
            self.read(**kwargs)  # Load the DataFrame if it hasn't been loaded yet
        sample = self.spark_df.sample(fraction=min(self.sample_fraction, 1.0), seed=123)
        null_counts = (
            sample
            .select([
                F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in sample.columns
            ])
            .collect()[0].asDict()
        )
        return {
                "connector_id":   self.connector_id,
                "connector_type": self.connector_type,
                "path":           self.path,
                "row_count":      self.spark_df.count(),
                "column_count":   len(self.spark_df.columns),
                "columns":        self.spark_df.columns,
                "schema":         self.spark_df.schema.jsonValue(),
                "null_counts_sample": null_counts,
                "sample_fraction": self.sample_fraction,
                }
    
    @safe_call
    @require_connection
    def infer_schema(self):
        """
        Sample of the file and get spark infer types. 
        """
        sample_df = (
            self.__base_reader()
            .option("inferSchema", "true")  # Ensure that Spark infers the schema when reading the sample, which will allow us to return an accurate StructType representing the structure of the CSV file.
            .load(self.path)
            .sample(fraction=min(self.sample_fraction, 1.0), seed=123)  # Read a sample of the data to infer the schema, using the specified sample fraction for better performance on large files.
        )
        return sample_df.schema
    
    # Retries
    
    def retry(self, func, max_retries:int = 3, backoff:float=1.5 , **kwargs) :
        if max_retries > 10:
            logger.warning(f"RETRY WARNING : CSVConnector {self.connector_id} retry attempt with max_retries={max_retries} may lead to long wait times. Consider using a lower value for max_retries to avoid excessive delays.") 
        delay = backoff
        last_exception = None 
        for attempt in range(1, max_retries + 1):
            try: 
                return func(**kwargs)
            except Exception as e:
                last_exception = e 
                logger.warning(f"RETRY ATTEMPT {attempt} : CSVConnector {self.connector_id} encountered an error: {str(e)}. Retrying in {delay:.2f} seconds...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
        logger.error(f"RETRY FAILED : CSVConnector {self.connector_id} failed to execute function after {max_retries} attempts. Last error: {str(last_exception)}")
        raise RuntimeError(f"CSVConnector {self.connector_id} failed to execute function after {max_retries} attempts. Last error: {str(last_exception)}") from last_exception
    
    def get_stats(self):
        """
        Return cumulative runtime metrics for this connector instance.
        """
        stats = dict(self.statistics)
        if stats["reads"] > 0:
            stats["average_read_time_ms"] = stats["total_read_ms"] / stats["reads"]
        if stats["writes"] > 0:
            stats["average_write_time_ms"] = stats["total_write_ms"] / stats["writes"]
        self.statistics["data_count"] = self.spark_df.count() if self.spark_df is not None else 0
        return stats
    
    def log_read_event(self):
        logger.info(
            f"READ EVENT : CSVConnector {self.connector_id} has read {self.statistics['rows_read']} total rows across {self.statistics['reads']} read operations. Average read time: {self.get_stats().get('average_read_time_ms', 0):.2f} ms per read."
        )

    # __ methods
    def __repr__(self): 
        return (
            f"CSVConnector(id={self.connector_id!r}, "
            f"path={self.path!r}, "
            f"connected={self.is_connected})"
        )