# PySpark Coding Challenge - Training Data Pipeline

A PySpark pipeline that transforms raw e-commerce data (impressions, clicks, add-to-carts, orders) into training-ready format for a PyTorch transformer model that powers the "Our top choices carousel" recommendation system.

## 🎯 Challenge Overview

This solution addresses the challenge of preparing training data for a transformer model that will improve product recommendations. The pipeline processes millions of daily impressions and customer actions to create temporally-aware training sequences.

## 🏗️ High-Level Architecture

### Training Input Structure

The pipeline produces training data in the exact format required by the PyTorch model:

```python
def forward(self, impressions: Tensor, actions: Tensor, action_types: Tensor) -> Tensor:
```

**Output Format:**
- **impressions**: `[batch_size]` - Single item ID per training sample
- **actions**: `[batch_size, 1000]` - Customer's last 1000 actions (item IDs)
- **action_types**: `[batch_size, 1000]` - Action type mapping (1=click, 2=cart, 3=order, 0=padding)

### Training Workflow

**Training Approach:**
The training runs on 14 days of data using PyTorch on a single large GPU, iterating over the full dataset one day at a time with multiple epochs. Within each day, impressions are exploded into individual training samples and randomly sampled.

**Pipeline Design:**
The pipeline transforms raw impression/action data into PyTorch-ready tensors. Each training sample represents one item shown to one customer, with that customer's most recent 1000 actions (going back 1+ years). The transformer model learns to predict `is_order` (purchase likelihood) based on item embeddings and customer behavioral sequences.

**Key Features:**
- **Daily Processing**: 14 days processed iteratively for temporal training
- **Impression Explosion**: ~10M training samples from 1M daily impression rows  
- **Action Sequences**: Customer's last 1000 actions in descending chronological order
- **Temporal Integrity**: Actions filtered to prevent data leakage (pre-impression only)
- **GPU Optimization**: Fixed-size tensors in Parquet format for efficient loading

## 📊 Data Pipeline Architecture

### High-Level Data Flow

```
Data Sources              Processing Layer           ML Training
┌─────────────┐          ┌─────────────────┐        ┌─────────────┐
│   Kafka     │─────────▶│   PySpark       │───────▶│  PyTorch    │
└─────────────┘          │   Pipeline      │        │ Transformer │
                         │                 │        └─────────────┘
┌─────────────┐          │                 │               │
│ Data Warehouse│───────▶│                 │               ▼
└─────────────┘          └─────────────────┘        ┌─────────────┐
                                  │                 │ Production  │
                                  ▼                 │   API       │
                         ┌─────────────────┐        └─────────────┘
                         │ Training Tensors │
                         └─────────────────┘
```

### Input Data Sources

1. **Impressions** (`dt=YYYY-MM-DD` partitioned)
   - Daily carousel items shown to customers (~1M rows/day)
   - Each row contains 10 items on average

2. **Clicks** (Kafka topic, `dt` partitioned) 
   - Customer item interactions (~150M/week)

3. **Add to Carts** (Kafka topic, `dt` partitioned)
   - Shopping cart additions (~15M/week)

4. **Previous Orders** (Data warehouse table)
   - Historical purchase data (~2M/week)

### Transformation Pipeline

```
   Raw Data Sources
         ↓↓
   Data Preparation
   (Schema validation, date filtering)
         ↓↓
   Action History Building
   (Union clicks + carts + orders)
         ↓↓
   Impression Explosion
   (Array → Individual training samples)
         ↓↓
   Temporal Join & Ranking
   (Customer actions up to impression date)
         ↓↓
   Sequence Padding
   (Exactly 1000 actions per sample)
         ↓↓
   Training Data Output
```

## 🚀 Performance Optimizations

### Scale Considerations
- **Customer Partitioning**: Data partitioned by `customer_id` for parallel processing
- **Window Functions**: Efficient ranking of actions by timestamp
- **Broadcast Joins**: Small lookup tables broadcast to all executors  
- **Columnar Storage**: Output in Parquet format for fast PyTorch loading
- **Memory Management**: Streaming processing for large action histories

### Compute Resource Efficiency
- **Predicate Pushdown**: Date filtering applied early in pipeline
- **Column Pruning**: Only required columns selected throughout pipeline
- **Caching Strategy**: Intermediate results cached for iterative processing
- **Dynamic Partitioning**: Spark partitions scaled based on data volume

### Production Spark Configuration

**Resource Allocation:**
- **Executor Memory**: 14GB (optimized for large window operations)
- **Executor Cores**: 4 (balanced parallelism vs overhead)
- **Executor Instances**: 2-20 (auto-scaling based on workload)
- **Shuffle Partitions**: 800 (scaled for data volume)

**Databricks Optimizations:**
- **Adaptive Query Execution**: Dynamic partition coalescing and skew handling
- **Delta Lake**: Optimized writes and auto-compaction
- **Arrow Integration**: Faster pandas/PyTorch data conversion
- **Cost-Based Optimizer**: Intelligent join reordering

## 📁 Project Structure

```
pyspark-coding-challenge/
├── src/
│   ├── __init__.py
│   ├── config.py           # Schema definitions and constants
│   ├── pipeline.py         # Main pipeline orchestration
│   ├── transformations.py  # Core transformation logic
│   ├── utils.py            # Utility functions for data loading
│   ├── spark_config.py     # Production Spark configurations
│   └── data/               # Sample data files
├── tests/
│   ├── __init__.py
│   ├── sample_data.py           # Test data generators
│   ├── test_pipeline.py         # Integration tests
│   └── test_transformations.py  # Unit tests
├── scripts/
│   ├── run_pipeline.sh       # Local pipeline execution
│   └── databricks_deploy.sh  # Databricks deployment script
├── requirements.txt          # Python dependencies
├── setup.py                  # Package configuration
└── README.md
```

## 🔧 Pipeline Components

### 1. Transformer Class (`src/transformations.py`)

**Core Methods:**
- `__prepare_impressions__()`: Sets reference date for temporal filtering
- `__explode_impressions__()`: Converts impression arrays to individual rows
- `__prepare_clicks__()`: Standardizes click data format
- `__prepare_add_to_carts__()`: Processes cart addition events
- `__prepare_previous_orders__()`: Handles order history data
- `build_action_history()`: Unions all action types into single DataFrame
- `join_impressions_with_actions()`: Creates final training samples

### 2. Data Schemas (`src/config.py`)

All schemas comply with the provided specifications:
- **Clicks**: `dt`, `customer_id`, `item_id`, `click_time`
- **Add to Carts**: `dt`, `customer_id`, `config_id`, `simple_id`, `occurred_at`
- **Orders**: `order_date` (DateType), `customer_id`, `config_id`, `simple_id`, `occurred_at`
- **Impressions**: `dt`, `ranking_id`, `customer_id`, `impressions[]`

### 3. Action Type Mapping

```python
ACTION_CLICK = 1    # Customer clicked item
ACTION_ATC = 2      # Customer added to cart  
ACTION_ORD = 3      # Customer ordered item
ACTION_NONE = 0     # Padding for sequences < 1000
```

## 📈 Training Data Output

### Format Specification

Each row in the output represents one training sample:

| Column | Type | Shape | Description |
|--------|------|--------|-------------|
| `dt` | String | - | Impression date (for daily iteration) |
| `customer_id` | Integer | - | Customer identifier |
| `ranking_id` | String | - | Impression session ID |
| `item_id` | Integer | - | Item being recommended (target) |
| `is_order` | Boolean | - | Training label (did customer order?) |
| `actions` | Array[Integer] | [1000] | Customer's action sequence (item IDs) |
| `action_types` | Array[Integer] | [1000] | Corresponding action types |

### Training Integration

**PyTorch DataLoader Integration:**
```python
# Example PyTorch usage
dataset = SparkDataset(training_parquet_path)
dataloader = DataLoader(dataset, batch_size=256, shuffle=True)

for batch in dataloader:
    impressions = batch['item_id']          # [256]
    actions = batch['actions']              # [256, 1000] 
    action_types = batch['action_types']    # [256, 1000]
    
    predictions = model(impressions, actions, action_types)
```

## 🧪 Testing Strategy

### Unit Tests (`tests/test_transformations.py`)
- **Data Explosion**: Validates impression array → individual samples
- **Temporal Filtering**: Ensures proper date-based filtering
- **Action Ordering**: Verifies most-recent-first sequencing
- **Array Padding**: Confirms exactly 1000 elements per sequence
- **Schema Compliance**: Validates output format

### Integration Tests (`tests/test_pipeline.py`)
- **End-to-End Pipeline**: Full workflow with realistic data
- **Daily Processing**: Date filtering for iterative training
- **PyTorch Compatibility**: Output format validation
- **Performance Testing**: Memory and compute efficiency

### Test Data
- **Temporal Scenarios**: Past and recent actions for filtering tests
- **Multi-Customer**: Different user behavior patterns
- **Edge Cases**: Users with no actions, minimal history

## 🚀 Usage

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run full pipeline
./scripts/run_pipeline.sh run

# Process specific date (for daily training)
./scripts/run_pipeline.sh daily 2024-12-15

# Run test suite
./scripts/run_pipeline.sh test
```

### Databricks Production Deployment

```bash
# Set up Databricks environment
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=dapi1234567890abcdef

# Install and configure Databricks CLI
./scripts/databricks_deploy.sh setup

# Deploy pipeline to production cluster
./scripts/databricks_deploy.sh deploy

# Run production pipeline
./scripts/databricks_deploy.sh run

# Monitor jobs and clusters
./scripts/databricks_deploy.sh monitor

# Clean up resources
./scripts/databricks_deploy.sh cleanup
```

### Python API

```python
from src.pipeline import main

# Process all data
training_df = main()

# Process specific date
daily_df = main(target_date="2024-12-15")

# Get training samples count
print(f"Generated {training_df.count()} training samples")
```

### Production API (Databricks)

```python
from src.pipeline import main
from src.spark_config import create_optimized_spark_session

# Production mode with optimized Spark configuration
training_df = main(prod_mode=True, verbose=True)

# Process specific date for daily training
daily_df = main(target_date="2024-12-15", prod_mode=True)

# Manual transformer usage
from src.transformations import Transformer
transformer = Transformer("ProductionTransformer")
final_df = transformer.join_impressions_with_actions(actions_df, impressions_df)
```

### Cluster Specifications

**Recommended Databricks Cluster:**
- **Node Type**: Standard_D16s_v3 (16 cores, 64GB RAM)
- **Workers**: 2-20 (auto-scaling)
- **Spark Version**: 13.3.x-scala2.12 (Latest LTS)
- **Auto-termination**: 30 minutes

**Memory Requirements by Scale:**
- **Daily Processing**: 200GB cluster memory (~15-30 minutes)
- **Weekly Processing**: 800GB cluster memory (~1-2 hours)  
- **Full Pipeline (14 days)**: 1.5TB cluster memory (~2-4 hours)

## 🎯 Key Features

### ✅ **Correctness**
- **Temporal integrity**: No data leakage, proper action sequencing
- **Spec compliance**: Exact PyTorch model format match
- **Action accuracy**: Customer's actual last 1000 actions

### ✅ **Performance** 
- **Scalable design**: Handles millions of daily impressions
- **Resource efficient**: Optimized Spark operations
- **Memory conscious**: Streaming and caching strategies

### ✅ **Production Ready**
- **Comprehensive testing**: Unit + integration test coverage
- **Error handling**: Graceful failure and recovery
- **Monitoring ready**: Structured logging and metrics

### ✅ **Maintainable**
- **Modular architecture**: Clear separation of concerns  
- **Type safety**: Strong typing throughout pipeline
- **Documentation**: Comprehensive code and API docs

### 🏗️ Infrastructure Requirements

**Databricks Cluster Configuration:**
```json
{
  "cluster_name": "pyspark-training-pipeline-prod",
  "node_type_id": "Standard_D16s_v3",
  "autoscale": {"min_workers": 2, "max_workers": 20},
  "spark_conf": {
    "spark.executor.memory": "14g",
    "spark.sql.shuffle.partitions": "800",
    "spark.sql.adaptive.enabled": "true"
  }
}
```

**Cost Optimization:**
- Auto-scaling clusters (2-20 workers) for variable loads
- 30-minute auto-termination for cost efficiency
- Spot instances for development workloads
- Delta Lake for storage optimization

## 📦 Package Installation

For advanced users who want to install the pipeline as a Python package:

```bash
# Install in development mode (editable)
pip install -e .

# Install as a regular package
pip install .

# Install with development dependencies
pip install -e .[dev]

# After installation, run pipeline from anywhere
pyspark-training-pipeline  # Equivalent to running src.pipeline:main()
```

**Benefits of package installation:**
- Pipeline becomes a system-wide command
- Clean dependency management
- Easy integration with CI/CD pipelines
- Portable across different environments
- Professional Python package structure

---

*This project demonstrates production-ready PySpark development with comprehensive testing, documentation, and deployment capabilities for the "Our top choices carousel" recommendation system.*