#!/bin/bash

# PySpark Training Data Pipeline Runner
# This script sets up the environment and runs the training data pipeline

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE} PySpark Training Data Pipeline Runner ${NC}"
echo -e "${BLUE}========================================${NC}"

# Check if we're in the right directory
if [ ! -f "src/pipeline.py" ]; then
    echo -e "${RED}Error: pipeline.py not found. Please run this script from the project root.${NC}"
    echo -e "${YELLOW}Expected directory structure:${NC}"
    echo -e "  pyspark-coding-challenge/"
    echo -e "    ├── src/pipeline.py"
    echo -e "    ├── scripts/run_pipeline.sh"
    echo -e "    └── requirements.txt"
    exit 1
fi

# Set PYTHONPATH to include src directory
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
echo -e "${YELLOW}PYTHONPATH set to: ${PYTHONPATH}${NC}"

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: Python 3 is required but not found.${NC}"
    echo -e "${YELLOW}Please install Python 3.8+ and try again.${NC}"
    exit 1
fi

echo -e "${YELLOW}Using Python: $(which python3) ($(python3 --version))${NC}"

# Function to check and install dependencies
check_dependencies() {
    echo -e "${YELLOW}Checking dependencies...${NC}"
    
    # Try importing pyspark
    if python3 -c "import pyspark" 2>/dev/null; then
        echo -e "${GREEN}✅ PySpark is available${NC}"
        return 0
    fi
    
    echo -e "${YELLOW}PySpark not found. Attempting to install dependencies...${NC}"
    
    # Try different installation methods
    if python3 -m pip install -r requirements.txt --break-system-packages 2>/dev/null; then
        echo -e "${GREEN}✅ Dependencies installed successfully with pip${NC}"
    elif python3 -m pip install -r requirements.txt --user 2>/dev/null; then
        echo -e "${GREEN}✅ Dependencies installed successfully with pip --user${NC}"
    elif pip3 install -r requirements.txt 2>/dev/null; then
        echo -e "${GREEN}✅ Dependencies installed successfully with pip3${NC}"
    else
        echo -e "${RED}❌ Failed to install dependencies automatically.${NC}"
        echo -e "${YELLOW}Please try one of these commands manually:${NC}"
        echo -e "  python3 -m pip install -r requirements.txt --user"
        echo -e "  python3 -m pip install -r requirements.txt --break-system-packages"
        echo -e "  pip3 install -r requirements.txt"
        echo ""
        echo -e "${YELLOW}Or create a virtual environment:${NC}"
        echo -e "  python3 -m venv venv"
        echo -e "  source venv/bin/activate"
        echo -e "  pip install -r requirements.txt"
        echo -e "  ./scripts/run_pipeline.sh run"
        exit 1
    fi
    
    # Test import again
    if ! python3 -c "import pyspark" 2>/dev/null; then
        echo -e "${RED}❌ PySpark still not available after installation.${NC}"
        echo -e "${YELLOW}Please check your Python environment and try manual installation.${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✅ All dependencies are available${NC}"
}

# Check dependencies
check_dependencies

# Function to run pipeline with date filter
run_pipeline() {
    local target_date=$1
    local mode=$2
    
    echo -e "${GREEN}Running pipeline...${NC}"
    if [ -n "$target_date" ]; then
        echo -e "${YELLOW}Filtering for date: ${target_date}${NC}"
        if [ "$mode" == "quiet" ]; then
            python3 -c "
import sys
sys.path.insert(0, 'src')
from pipeline import main
try:
    result = main('$target_date', verbose=False)
    print(f'✅ Pipeline completed successfully! Generated {result.count()} training samples.')
except Exception as e:
    print(f'❌ Pipeline failed: {e}')
    sys.exit(1)
"
        else
            python3 -c "
import sys
sys.path.insert(0, 'src')
from pipeline import main
try:
    result = main('$target_date')
    print(f'✅ Pipeline completed successfully!')
except Exception as e:
    print(f'❌ Pipeline failed: {e}')
    sys.exit(1)
"
        fi
    else
        echo -e "${YELLOW}Processing all dates${NC}"
        if [ "$mode" == "quiet" ]; then
            python3 -c "
import sys
sys.path.insert(0, 'src')
from pipeline import main
try:
    result = main(verbose=False)
    print(f'✅ Pipeline completed successfully! Generated {result.count()} training samples.')
except Exception as e:
    print(f'❌ Pipeline failed: {e}')
    sys.exit(1)
"
        else
            python3 -c "
import sys
sys.path.insert(0, 'src')
from pipeline import main
try:
    result = main()
    print(f'✅ Pipeline completed successfully!')
except Exception as e:
    print(f'❌ Pipeline failed: {e}')
    sys.exit(1)
"
        fi
    fi
}

# Function to run tests
run_tests() {
    echo -e "${GREEN}Running tests...${NC}"
    
    # Check if pytest is available
    if command -v pytest &> /dev/null; then
        echo -e "${YELLOW}Using pytest command${NC}"
        pytest tests/ -v
    elif python3 -c "import pytest" 2>/dev/null; then
        echo -e "${YELLOW}Using python -m pytest${NC}"
        python3 -m pytest tests/ -v
    else
        echo -e "${YELLOW}pytest not found, running tests with unittest${NC}"
        python3 -m unittest discover tests/ -v
    fi
}

# Parse command line arguments
case "$1" in
    "test")
        echo -e "${YELLOW}🧪 Running test suite...${NC}"
        run_tests
        ;;
    "run")
        TARGET_DATE=$2
        if [ -n "$TARGET_DATE" ]; then
            echo -e "${YELLOW}🚀 Running pipeline for date: $TARGET_DATE${NC}"
        else
            echo -e "${YELLOW}🚀 Running full pipeline...${NC}"
        fi
        run_pipeline "$TARGET_DATE"
        ;;
    "daily")
        if [ -z "$2" ]; then
            echo -e "${RED}Error: Please provide a date for daily processing${NC}"
            echo -e "${YELLOW}Usage: $0 daily YYYY-MM-DD${NC}"
            echo -e "${YELLOW}Example: $0 daily 2024-12-15${NC}"
            exit 1
        fi
        echo -e "${YELLOW}🗓️  Running daily pipeline for: $2${NC}"
        run_pipeline "$2" "quiet"
        ;;
    "help"|"--help"|"-h")
        echo -e "${BLUE}PySpark Training Data Pipeline${NC}"
        echo -e "${YELLOW}Usage: $0 {run|daily|test|help}${NC}"
        echo ""
        echo -e "${YELLOW}Commands:${NC}"
        echo -e "  run              - Run the full pipeline on all data"
        echo -e "  daily DATE       - Run pipeline for specific date (YYYY-MM-DD)"
        echo -e "  test             - Run the test suite with pytest"
        echo -e "  help             - Show this help message"
        echo ""
        echo -e "${YELLOW}Examples:${NC}"
        echo -e "  $0 run                    # Process all data"
        echo -e "  $0 daily 2024-12-15       # Process specific date"
        echo -e "  $0 test                   # Run tests"
        echo ""
        echo -e "${YELLOW}Dependencies:${NC}"
        echo -e "  The script will automatically try to install required packages."
        echo -e "  If installation fails, manually run: pip install -r requirements.txt"
        ;;
    "")
        echo -e "${YELLOW}No command provided. Use '$0 help' for usage information${NC}"
        exit 1
        ;;
    *)
        echo -e "${RED}Unknown command: $1${NC}"
        echo -e "${YELLOW}Use '$0 help' for usage information${NC}"
        exit 1
        ;;
esac

echo -e "${GREEN}🎉 Done!${NC}"