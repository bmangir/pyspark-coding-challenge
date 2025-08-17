"""
Spark Configuration for Production Databricks Deployment

This module contains optimized Spark configurations for processing:
- ~1M daily impressions (14 days = 14M impressions)
- ~150M weekly actions (2+ weeks = 300M+ actions)
- ~10M active users per month
- Joins and window operations at scale
"""

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


def get_production_spark_config():
    """
    Production Spark configuration optimized for large-scale data processing on Databricks clusters.
    
    Scale assumptions:
    - Daily data: 1M impressions × 14 days = 14M training samples
    - Action data: 150M clicks + 15M carts + 2M orders per week
    - Memory: Large window operations require significant RAM
    - Compute: Customer partitioning across many cores
    """
    conf = SparkConf()
    
    # === RESOURCE ALLOCATION === #
    # Optimized for Databricks Standard_D16s_v3 or similar
    conf.set("spark.executor.memory", "8g")           # High memory for window operations
    conf.set("spark.executor.cores", "4")             # Balance parallelism vs overhead
    conf.set("spark.executor.instances", "100")       # Scale based on cluster size
    conf.set("spark.driver.memory", "8g")             # Driver coordinates large jobs
    conf.set("spark.driver.cores", "2")

    # === MEMORY MANAGEMENT === #
    conf.set("spark.executor.memoryFraction", "0.8")   # 80% for caching/execution
    conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")  # Faster pandas conversion
    conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
    
    # === SHUFFLE OPTIMIZATION === #
    conf.set("spark.sql.shuffle.partitions", "800")    # Scale with data volume
    conf.set("spark.sql.adaptive.enabled", "true")     # Dynamic optimization
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    
    # === PERFORMANCE OPTIMIZATIONS === #
    conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
    conf.set("spark.sql.cbo.enabled", "true")          # Cost-based optimizer
    conf.set("spark.sql.cbo.joinReorder.enabled", "true")
    conf.set("spark.sql.statistics.histogram.enabled", "true")
    
    # === CACHING STRATEGY === #
    conf.set("spark.sql.cache.serializer", "org.apache.spark.sql.columnar.CachedBatchSerializer")
    conf.set("spark.sql.columnVector.offheap.enabled", "true")
    
    # === DATABRICKS OPTIMIZATIONS === #
    conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    conf.set("spark.databricks.delta.autoCompact.enabled", "true")
    conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
    
    # === CUSTOM OPTIMIZATIONS FOR OUR PIPELINE === #
    # Window operations are expensive - optimize memory
    conf.set("spark.sql.windowExec.buffer.in.memory.threshold", "4096")
    conf.set("spark.sql.windowExec.buffer.spill.threshold", "2147483647")
    
    # Broadcasting for small lookup tables
    conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")
    
    return conf


def get_databricks_cluster_config():
    """
    Recommended Databricks cluster configuration for production deployment.
    
    Returns cluster specification for terraform/API deployment.
    """
    return {
        "cluster_name": "pyspark-training-pipeline-prod",
        "spark_version": "13.3.x-scala2.12",  # Latest LTS with Delta Lake
        "node_type_id": "Standard_D16s_v3",   # 16 cores, 64GB RAM
        "driver_node_type_id": "Standard_D8s_v3",  # 8 cores, 32GB RAM
        "num_workers": 5,                     # Auto-scaling: 2-20 workers
        "autoscale": {
            "min_workers": 2,
            "max_workers": 20
        },
        "spark_conf": {
            "spark.executor.memory": "14g",
            "spark.executor.cores": "4", 
            "spark.sql.shuffle.partitions": "800",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.databricks.delta.optimizeWrite.enabled": "true",
            "spark.sql.execution.arrow.pyspark.enabled": "true"
        },
        "spark_env_vars": {
            "PYTHONPATH": "/databricks/python3/lib/python3.11/site-packages",
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
        "custom_tags": {
            "project": "training-data-pipeline",
            "environment": "production",
            "cost-center": "ml-engineering"
        },
        "init_scripts": [{
            "workspace": {
                "destination": "/databricks/init-scripts/install-dependencies.sh"
            }
        }]
    }


def create_optimized_spark_session(app_name="PySpark-Training-Pipeline"):
    """
    Create a Spark session with production-optimized configuration.
    
    Args:
        app_name: Name for the Spark application
        
    Returns:
        SparkSession: Configured Spark session
    """
    conf = get_production_spark_config()
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()
    
    # Additional runtime optimizations
    spark.sparkContext.setLogLevel("WARN")  # Reduce log noise
    
    return spark


def get_memory_estimates():
    """
    Memory requirement estimates for different data scales.
    
    Returns estimates for cluster sizing decisions.
    """
    return {
        "daily_processing": {
            "data_volume": "1M impressions + 21M actions",
            "memory_required": "200GB cluster memory",
            "processing_time": "15-30 minutes",
            "recommended_cluster": "Standard_D16s_v3 × 3 workers"
        },
        "weekly_processing": {
            "data_volume": "7M impressions + 150M actions", 
            "memory_required": "800GB cluster memory",
            "processing_time": "1-2 hours",
            "recommended_cluster": "Standard_D16s_v3 × 10 workers"
        },
        "full_pipeline": {
            "data_volume": "14M impressions + 300M actions",
            "memory_required": "1.5TB cluster memory", 
            "processing_time": "2-4 hours",
            "recommended_cluster": "Standard_D16s_v3 × 20 workers"
        }
    }


# DATABRICKS DEPLOYMENT HELPER FUNCTIONS

def get_databricks_job_config():
    """
    Databricks Job configuration for scheduled pipeline runs. (Theoretical example)
    """
    return {
        "name": "Daily Training Data Pipeline",
        "email_notifications": {
            "on_start": ["ml-team@company.com"],
            "on_success": ["ml-team@company.com"], 
            "on_failure": ["ml-team@company.com", "oncall@company.com"]
        },
        "timeout_seconds": 14400,
        "max_retries": 2,
        "min_retry_interval_millis": 60000,  # 1 minute
        "retry_on_timeout": True,
        "schedule": {
            "quartz_cron_expression": "0 0 6 * * ?",  # Daily at 6 AM UTC
            "timezone_id": "UTC"
        },
        "spark_python_task": {
            "python_file": "dbfs:/FileStore/shared_uploads/pipeline/main.py",
            "parameters": ["--mode", "production", "--date", "{{ ds }}"]
        },
        "new_cluster": get_databricks_cluster_config()
    }