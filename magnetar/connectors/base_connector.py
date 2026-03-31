"""
Base connector class for Magnetar. This class defines the interface that all connectors must implement. It also provides some common functionality that can be shared across different connectors.
"""

import random
import string
from time import time


class BaseConnector:
    def __init__(self, **kwargs):
        self.connector_id : str = kwargs.get("connector_id", ''.join(random.choices(string.ascii_letters + string.digits, k=8)))
        self.connector_type : str = kwargs.get("connector_type", "base")

    # Core I/O methods that all connectors must implement
    def read(self, **kwargs):
        """
        The data loader from the source. 
        """
        pass 

    def yield_data(self, **kwargs):
        """
        A generator that yields data in batches. This is useful for large datasets that cannot fit into memory.
        """
        pass

    def write(self, data, destination, **kwargs):
        """
        The data writer to the destination. 
        """
        pass
    
    def read_schema(self, **kwargs):
        """
        Return the source schema before loading the data for early validation. 
        """
        pass

    # Spark Integration methods 
    def to_spark(self, spark_session, **kwargs):
        """
        Convert the data to a Spark DF
        """
        pass

    def to_rdd(self, spark_session, **kwargs):
        """
        Convert the data to a Spark RDD
        """
        pass

    def get_spark_schema(self, **kwargs):
        """
        Return the spark schema. 
        """
        pass

    def read_partitioned(self, partition_col, num_partitions, **kwargs):
        """
        Read the data in a partitioned way. This is useful for large datasets that can be read in parallel.
        Only for RD_connectors
        """
        pass

    # Validation and Introspection 
    def validate_config(self) : 
        """
        Check that all required config keys are present and well-formed. 
        """
        pass

    def test_connection(self) : 
        """
        Ping the source to verify connectivity
        """
        pass

    def get_metadata(self) : 
        """
        Return metadata about the source, such as number of records, size, etc. 
        """
        pass

    def infer_schema(self): 
        """
        Sample a subset of the data and infer column types. 
        """
        pass

    # Lifecycle and Reliability
    def connect(self) : 
        """
        Establish a connection to the source. This can be used to set up any necessary authentication or session management. 
        """
        pass

    def disconnect(self) : 
        """
        Clean up any resources or connections established in the connect method. 
        """
        pass

    def __enter__(self):
        self.connect() # For with statement support
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.disconnect()

    def retry(self, func, max_retries=3, **kwargs):
        """
        A simple retry mechanism for handling transient errors. 
        """
        pass

    # Observability and metrics 
    def get_stats(self): 
        """
        Getting stats of runtime metrics. 
        """
        pass

    def log_read_event(self): 
        """
        Structured log and metrics
        """
        pass