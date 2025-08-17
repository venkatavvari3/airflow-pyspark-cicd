# PySpark Airflow CI/CD Pipeline

A comprehensive CI/CD pipeline for PySpark jobs orchestrated by Apache Airflow using GitHub Actions.

## Project Structure

```
airflow-pyspark-cicd/
├── .github/
│   └── workflows/
│       └── ci-cd-pipeline.yml        # Main CI/CD pipeline
├── src/
│   └── jobs/
│       ├── data_ingestion.py         # PySpark data ingestion job
│       ├── data_transformation.py    # PySpark data transformation job
│       └── data_quality.py           # PySpark data quality checks
├── dags/
│   └── pyspark_data_pipeline.py      # Airflow DAG
├── tests/
│   ├── unit/                         # Unit tests
│   ├── integration/                  # Integration tests
│   ├── dags/                         # DAG tests
│   └── smoke/                        # Smoke tests
├── docker/
│   ├── Dockerfile.pyspark            # PySpark container
│   └── Dockerfile.airflow            # Airflow container
├── config/                           # Configuration files
├── requirements.txt                  # Python dependencies
├── requirements-dev.txt              # Development dependencies
└── docker-compose.yml                # Local development setup
```

## Features

### CI/CD Pipeline
- **Automated Testing**: Unit, integration, and smoke tests
- **Code Quality**: Linting with flake8, pylint, type checking with mypy
- **Security Scanning**: Safety and bandit security scans
- **Multi-Environment Deployment**: Dev, staging, and production
- **Docker Build**: Automated container builds and registry pushes
- **Kubernetes Deployment**: EKS deployment with rolling updates

### PySpark Jobs
- **Data Ingestion**: Reads raw data and performs initial cleaning
- **Data Transformation**: Applies business logic and creates aggregations
- **Data Quality**: Validates data quality with comprehensive checks
- **Scalable Processing**: Optimized Spark configurations

### Airflow Orchestration
- **EMR Integration**: Dynamic cluster creation and termination
- **Monitoring**: Built-in notifications and error handling
- **Scheduling**: Configurable scheduling with dependency management
- **Retry Logic**: Automatic retries with exponential backoff

## Quick Start

### Local Development

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd airflow-pyspark-cicd
   ```

2. **Set up environment**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   pip install -r requirements-dev.txt
   ```

4. **Install Java**

   - Ensure Java 11 is installed and `JAVA_HOME` is set.

5. **Install and Configure Hadoop Winutils (Windows Only)**

   - Download `winutils.exe` for Hadoop 3.3.x from [cdarlint/winutils releases](https://github.com/cdarlint/winutils/).
   - Create a directory: `C:\hadoop\bin`
   - Place `winutils.exe` in `C:\hadoop\bin`
   - Set environment variables:
     - `HADOOP_HOME` to `C:\hadoop`
     - Add `C:\hadoop\bin` to your system `PATH`

6. **Install Microsoft Visual C++ Redistributable (Windows Only)**

   - Download and install both [x64](https://aka.ms/vs/17/release/vc_redist.x64.exe) and [x86](https://aka.ms/vs/17/release/vc_redist.x86.exe) versions.
   - Restart your computer after installation.

7. **Verify winutils.exe (Windows Only)**

   Open **Command Prompt** (not PowerShell) and run:
   ```cmd
   C:\hadoop\bin\winutils.exe ls
   ```
   If you see a directory listing (not a DLL error), your setup is correct.

8. **Run PySpark Tests**

   ```bash
   pytest tests/unit/ -v
   pytest tests/integration/ -v
   ```

   > **Note:**  
   > If you see errors like `ExitCodeException exitCode=-1073741515`, it means a required DLL is missing. Double-check the steps above, especially the Visual C++ Redistributable and winutils.exe setup.

9. **Start local Airflow**
   ```bash
   docker-compose up -d
   ```
   Access Airflow UI at http://localhost:8080 (admin/admin)

### Running PySpark Jobs Locally

```bash
# Data Ingestion
python -m src.jobs.data_ingestion \
  --input-path /path/to/input \
  --output-path /path/to/output \
  --date 2024-01-01

# Data Transformation
python -m src.jobs.data_transformation \
  --input-path /path/to/processed \
  --output-path /path/to/transformed \
  --date 2024-01-01

# Data Quality
python -m src.jobs.data_quality \
  --input-path /path/to/transformed \
  --date 2024-01-01
```

## Configuration

### GitHub Secrets

Configure the following secrets in your GitHub repository:

#### Container Registry
- `CONTAINER_REGISTRY`: Container registry URL
- `REGISTRY_USERNAME`: Registry username
- `REGISTRY_PASSWORD`: Registry password

#### AWS Configuration
- `AWS_ACCESS_KEY_ID`: AWS access key
- `AWS_SECRET_ACCESS_KEY`: AWS secret key
- `AWS_REGION`: AWS region
- `EKS_CLUSTER_NAME_DEV`: Development EKS cluster name
- `EKS_CLUSTER_NAME_STAGING`: Staging EKS cluster name
- `EKS_CLUSTER_NAME_PROD`: Production EKS cluster name

#### Production Deployment
- `AWS_ACCESS_KEY_ID_PROD`: Production AWS access key
- `AWS_SECRET_ACCESS_KEY_PROD`: Production AWS secret key
- `PROD_APPROVERS`: Comma-separated list of GitHub usernames for production approvals

### Environment Variables

```bash
# Spark Configuration
SPARK_VERSION=3.4.0
HADOOP_VERSION=3.3
PYTHON_VERSION=3.9

# AWS Configuration
AWS_REGION=us-east-1
AWS_S3_BUCKET=your-data-bucket

# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=airflow_dev
```

## Pipeline Workflow

### 1. Code Quality & Testing
- **Linting**: flake8, pylint checks
- **Type Checking**: mypy validation
- **Unit Tests**: Individual component testing
- **Integration Tests**: End-to-end pipeline testing
- **Security Scans**: Safety and bandit checks

### 2. Build & Package
- **Docker Images**: Build PySpark and Airflow containers
- **Registry Push**: Push to container registry
- **Artifact Storage**: Store build artifacts

### 3. Deployment
- **Development**: Automatic deployment on develop branch
- **Staging**: Automatic deployment on main branch
- **Production**: Manual approval required

### 4. Monitoring & Validation
- **Smoke Tests**: Basic functionality verification
- **Health Checks**: Service availability monitoring
- **Rollback**: Automatic rollback on failure

## Data Quality Framework

The pipeline includes comprehensive data quality checks:

### Completeness Checks
- Null value detection
- Missing data analysis
- Completeness rate calculation

### Validity Checks
- Data type validation
- Range checks (e.g., positive prices)
- Format validation

### Consistency Checks
- Cross-field validation
- Business rule enforcement
- Calculated field verification

### Integrity Checks
- Duplicate detection
- Referential integrity
- Uniqueness constraints

## Monitoring & Alerting

### Built-in Notifications
- Email alerts on pipeline failure
- Success notifications
- Deployment status updates

### Metrics Collection
- Pipeline execution metrics
- Data quality scores
- Performance monitoring

### Logging
- Structured logging with correlation IDs
- Centralized log aggregation
- Error tracking and analysis

## Best Practices

### Code Quality
- Follow PEP 8 style guidelines
- Use type hints for better code documentation
- Implement comprehensive error handling
- Write meaningful commit messages

### Testing
- Maintain high test coverage (>80%)
- Write tests before implementing features
- Use meaningful test data
- Test error conditions

### Security
- Regular dependency updates
- Security scanning in CI/CD
- Secrets management
- Access control and permissions

### Performance
- Optimize Spark configurations
- Monitor resource usage
- Implement data partitioning
- Use appropriate file formats

## Troubleshooting

### Common Issues

#### Pipeline Failures
1. Check GitHub Actions logs
2. Verify environment variables
3. Check resource quotas
4. Review error messages

#### Spark Job Failures
1. Check Spark UI logs
2. Verify input data format
3. Check resource allocation
4. Review data quality issues

#### Airflow Issues
1. Check Airflow logs
2. Verify DAG syntax
3. Check connections and variables
4. Review task dependencies

#### PySpark on Windows
- Ensure you have installed the correct `winutils.exe` for Hadoop 3.3.x.
- Install the Microsoft Visual C++ Redistributable (both x64 and x86).
- Set `HADOOP_HOME` and add `C:\hadoop\bin` to your `PATH`.
- Test `winutils.exe` from Command Prompt: `C:\hadoop\bin\winutils.exe ls`
- If you see `ExitCodeException exitCode=-1073741515`, a required DLL is missing.

### Getting Help

1. Check the GitHub Issues for known problems
2. Review the documentation
3. Contact the data engineering team
4. Submit a support ticket

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.