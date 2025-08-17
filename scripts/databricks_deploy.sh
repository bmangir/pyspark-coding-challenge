#!/bin/bash

# Databricks Deployment Script for PySpark Training Pipeline
# Handles cluster creation, job scheduling, and production deployment

set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
DATABRICKS_HOST="${DATABRICKS_HOST:-}"
DATABRICKS_TOKEN="${DATABRICKS_TOKEN:-}"
CLUSTER_NAME="pyspark-training-pipeline-prod"
JOB_NAME="Daily Training Data Pipeline"

# Function to print usage
usage() {
    echo -e "${YELLOW}Usage: $0 {setup|deploy|run|monitor|cleanup|help}${NC}"
    echo ""
    echo -e "${BLUE}Commands:${NC}"
    echo "  setup     : Install Databricks CLI and validate credentials"
    echo "  deploy    : Deploy pipeline to Databricks workspace"
    echo "  run       : Run the training pipeline job"
    echo "  monitor   : Monitor running jobs and cluster status"
    echo "  cleanup   : Terminate clusters and clean up resources"
    echo "  help      : Display this help message"
    echo ""
    echo -e "${BLUE}Environment Variables:${NC}"
    echo "  DATABRICKS_HOST  : Databricks workspace URL (required)"
    echo "  DATABRICKS_TOKEN : Personal access token (required)"
    echo ""
    echo -e "${BLUE}Example:${NC}"
    echo "  export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com"
    echo "  export DATABRICKS_TOKEN=dapi1234567890abcdef"
    echo "  ./scripts/databricks_deploy.sh setup"
    exit 1
}

# Validate environment
check_environment() {
    if [ -z "$DATABRICKS_HOST" ] || [ -z "$DATABRICKS_TOKEN" ]; then
        echo -e "${RED}Error: DATABRICKS_HOST and DATABRICKS_TOKEN must be set${NC}"
        echo "Set these environment variables and try again."
        exit 1
    fi
    
    if ! command -v databricks &> /dev/null; then
        echo -e "${RED}Error: Databricks CLI not found${NC}"
        echo "Run '$0 setup' to install the CLI first."
        exit 1
    fi
}

# Setup Databricks CLI
setup_databricks() {
    echo -e "${GREEN}Setting up Databricks CLI...${NC}"
    
    # Install Databricks CLI if not present
    if ! command -v databricks &> /dev/null; then
        echo "Installing Databricks CLI..."
        pip install databricks-cli
    fi
    
    # Configure authentication
    echo "Configuring Databricks authentication..."
    databricks configure --token << EOF
${DATABRICKS_HOST}
${DATABRICKS_TOKEN}
EOF
    
    # Test connection
    echo "Testing connection..."
    if databricks workspace ls /; then
        echo -e "${GREEN}✅ Successfully connected to Databricks workspace${NC}"
    else
        echo -e "${RED}❌ Failed to connect to Databricks workspace${NC}"
        exit 1
    fi
}

# Upload pipeline files to Databricks
upload_files() {
    echo -e "${GREEN}Uploading pipeline files to Databricks...${NC}"
    
    # Create workspace directory
    databricks workspace mkdirs /Shared/training-pipeline
    
    # Upload Python files
    databricks workspace import src/pipeline.py /Shared/training-pipeline/pipeline.py --format=SOURCE
    databricks workspace import src/transformations.py /Shared/training-pipeline/transformations.py --format=SOURCE
    databricks workspace import src/config.py /Shared/training-pipeline/config.py --format=SOURCE
    databricks workspace import src/utils.py /Shared/training-pipeline/utils.py --format=SOURCE
    databricks workspace import src/spark_config.py /Shared/training-pipeline/spark_config.py --format=SOURCE
    
    # Upload requirements and setup
    databricks workspace import requirements.txt /Shared/training-pipeline/requirements.txt --format=AUTO
    databricks workspace import setup.py /Shared/training-pipeline/setup.py --format=SOURCE
    
    # Create init script for dependencies
    cat > /tmp/init_script.sh << 'EOF'
#!/bin/bash
# Install pipeline dependencies
pip install --upgrade pip
pip install -r /dbfs/FileStore/shared_uploads/training-pipeline/requirements.txt
EOF
    
    databricks fs cp /tmp/init_script.sh dbfs:/FileStore/shared_uploads/training-pipeline/init_script.sh
    
    echo -e "${GREEN}✅ Files uploaded successfully${NC}"
}

# Create or update cluster
create_cluster() {
    echo -e "${GREEN}Creating production cluster...${NC}"
    
    # Check if cluster exists
    CLUSTER_ID=$(databricks clusters list --output JSON | jq -r ".clusters[] | select(.cluster_name==\"$CLUSTER_NAME\") | .cluster_id" || echo "")
    
    if [ -n "$CLUSTER_ID" ]; then
        echo "Cluster '$CLUSTER_NAME' already exists (ID: $CLUSTER_ID)"
        echo "Restarting cluster..."
        databricks clusters restart --cluster-id "$CLUSTER_ID"
    else
        echo "Creating new cluster '$CLUSTER_NAME'..."
        
        # Create cluster configuration
        cat > /tmp/cluster_config.json << 'EOF'
{
  "cluster_name": "pyspark-training-pipeline-prod",
  "spark_version": "13.3.x-scala2.12",
  "node_type_id": "Standard_D16s_v3",
  "driver_node_type_id": "Standard_D8s_v3",
  "autoscale": {
    "min_workers": 2,
    "max_workers": 20
  },
  "auto_termination_minutes": 30,
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
    "PYTHONPATH": "/databricks/python3/lib/python3.9/site-packages"
  },
  "custom_tags": {
    "project": "training-data-pipeline",
    "environment": "production"
  },
  "init_scripts": [{
    "dbfs": {
      "destination": "dbfs:/FileStore/shared_uploads/training-pipeline/init_script.sh"
    }
  }]
}
EOF
        
        CLUSTER_ID=$(databricks clusters create --json-file /tmp/cluster_config.json | jq -r .cluster_id)
        echo "Created cluster with ID: $CLUSTER_ID"
    fi
    
    # Wait for cluster to be ready
    echo "Waiting for cluster to start..."
    while true; do
        STATUS=$(databricks clusters get --cluster-id "$CLUSTER_ID" | jq -r .state)
        if [ "$STATUS" = "RUNNING" ]; then
            echo -e "${GREEN}✅ Cluster is running${NC}"
            break
        elif [ "$STATUS" = "TERMINATED" ] || [ "$STATUS" = "ERROR" ]; then
            echo -e "${RED}❌ Cluster failed to start (Status: $STATUS)${NC}"
            exit 1
        else
            echo "Cluster status: $STATUS (waiting...)"
            sleep 30
        fi
    done
    
    echo "$CLUSTER_ID" > /tmp/cluster_id.txt
}

# Create or update job
create_job() {
    echo -e "${GREEN}Creating/updating pipeline job...${NC}"
    
    # Check if job exists
    JOB_ID=$(databricks jobs list --output JSON | jq -r ".jobs[] | select(.settings.name==\"$JOB_NAME\") | .job_id" || echo "")
    
    CLUSTER_ID=$(cat /tmp/cluster_id.txt 2>/dev/null || echo "")
    if [ -z "$CLUSTER_ID" ]; then
        echo -e "${RED}Error: Cluster ID not found. Run cluster creation first.${NC}"
        exit 1
    fi
    
    # Create job configuration
    cat > /tmp/job_config.json << EOF
{
  "name": "$JOB_NAME",
  "email_notifications": {
    "on_failure": ["ml-team@company.com"]
  },
  "timeout_seconds": 14400,
  "max_retries": 2,
  "min_retry_interval_millis": 60000,
  "retry_on_timeout": true,
  "schedule": {
    "quartz_cron_expression": "0 0 6 * * ?",
    "timezone_id": "UTC"
  },
  "spark_python_task": {
    "python_file": "/Shared/training-pipeline/pipeline.py",
    "parameters": ["--mode", "production"]
  },
  "existing_cluster_id": "$CLUSTER_ID"
}
EOF
    
    if [ -n "$JOB_ID" ]; then
        echo "Updating existing job (ID: $JOB_ID)..."
        databricks jobs reset --job-id "$JOB_ID" --json-file /tmp/job_config.json
    else
        echo "Creating new job..."
        JOB_ID=$(databricks jobs create --json-file /tmp/job_config.json | jq -r .job_id)
        echo "Created job with ID: $JOB_ID"
    fi
    
    echo "$JOB_ID" > /tmp/job_id.txt
    echo -e "${GREEN}✅ Job configured successfully${NC}"
}

# Run the pipeline
run_pipeline() {
    check_environment
    
    JOB_ID=$(cat /tmp/job_id.txt 2>/dev/null || echo "")
    if [ -z "$JOB_ID" ]; then
        echo -e "${RED}Error: Job ID not found. Deploy the pipeline first.${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}Running training pipeline job...${NC}"
    RUN_ID=$(databricks jobs run-now --job-id "$JOB_ID" | jq -r .run_id)
    
    echo "Job started with run ID: $RUN_ID"
    echo "Monitor progress at: $DATABRICKS_HOST/#job/$JOB_ID/run/$RUN_ID"
    
    # Monitor run progress
    echo "Monitoring job progress..."
    while true; do
        RUN_STATUS=$(databricks runs get --run-id "$RUN_ID" | jq -r .state.life_cycle_state)
        RESULT_STATE=$(databricks runs get --run-id "$RUN_ID" | jq -r .state.result_state 2>/dev/null || echo "null")
        
        if [ "$RUN_STATUS" = "TERMINATED" ]; then
            if [ "$RESULT_STATE" = "SUCCESS" ]; then
                echo -e "${GREEN}✅ Pipeline completed successfully${NC}"
                break
            else
                echo -e "${RED}❌ Pipeline failed (Result: $RESULT_STATE)${NC}"
                exit 1
            fi
        else
            echo "Job status: $RUN_STATUS (waiting...)"
            sleep 60
        fi
    done
}

# Monitor clusters and jobs
monitor() {
    check_environment
    
    echo -e "${BLUE}=== Cluster Status ===${NC}"
    databricks clusters list --output JSON | jq -r '.clusters[] | select(.cluster_name | contains("training")) | "\(.cluster_name): \(.state)"'
    
    echo -e "\n${BLUE}=== Recent Job Runs ===${NC}"
    databricks runs list --limit 10 --output JSON | jq -r '.runs[] | select(.job_id != null) | "\(.start_time): \(.state.life_cycle_state) - \(.state.result_state // "RUNNING")"'
    
    echo -e "\n${BLUE}=== Active Jobs ===${NC}"
    databricks jobs list --output JSON | jq -r '.jobs[] | select(.settings.name | contains("Training")) | "\(.job_id): \(.settings.name)"'
}

# Cleanup resources
cleanup() {
    check_environment
    
    echo -e "${YELLOW}Cleaning up Databricks resources...${NC}"
    
    # Terminate clusters
    CLUSTER_ID=$(cat /tmp/cluster_id.txt 2>/dev/null || echo "")
    if [ -n "$CLUSTER_ID" ]; then
        echo "Terminating cluster $CLUSTER_ID..."
        databricks clusters delete --cluster-id "$CLUSTER_ID"
    fi
    
    # List clusters for manual cleanup
    echo -e "\n${BLUE}Remaining clusters:${NC}"
    databricks clusters list --output JSON | jq -r '.clusters[] | select(.cluster_name | contains("training")) | "\(.cluster_id): \(.cluster_name) (\(.state))"'
    
    echo -e "\n${GREEN}✅ Cleanup completed${NC}"
    echo "Note: Jobs are preserved for future runs. Delete manually if needed."
}

# Full deployment
deploy() {
    echo -e "${GREEN}Starting full Databricks deployment...${NC}"
    
    check_environment
    upload_files
    create_cluster
    create_job
    
    echo -e "\n${GREEN}🚀 Deployment completed successfully!${NC}"
    echo -e "\n${BLUE}Next steps:${NC}"
    echo "1. Run pipeline: ./scripts/databricks_deploy.sh run"
    echo "2. Monitor jobs: ./scripts/databricks_deploy.sh monitor"
    echo "3. View workspace: $DATABRICKS_HOST/#workspace/Shared/training-pipeline"
    
    CLUSTER_ID=$(cat /tmp/cluster_id.txt)
    JOB_ID=$(cat /tmp/job_id.txt)
    echo "4. Cluster URL: $DATABRICKS_HOST/#setting/clusters/$CLUSTER_ID/configuration"
    echo "5. Job URL: $DATABRICKS_HOST/#job/$JOB_ID"
}

# Main execution
case "$1" in
    setup)
        setup_databricks
        ;;
    deploy)
        deploy
        ;;
    run)
        run_pipeline
        ;;
    monitor)
        monitor
        ;;
    cleanup)
        cleanup
        ;;
    help)
        usage
        ;;
    *)
        echo -e "${RED}Invalid command: $1${NC}"
        usage
        ;;
esac
