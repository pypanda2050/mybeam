# Apache Beam Data Processing Pipelines

This project contains two Apache Beam pipelines for processing data from various sources and writing to Google Cloud Storage (GCS).

## Overview

The project includes two interconnected Apache Beam pipelines:

1. **PostgresToGcsPipeline** - Extracts data from PostgreSQL and writes to GCS. The output of this pipeline serves as Input 2 for the GcsProcessingPipeline.
2. **GcsProcessingPipeline** - Processes CSV files from GCS (two inputs), performs deduplication, and generates summary statistics. Input 1 comes from external GCS sources, while Input 2 comes from the PostgresToGcsPipeline output.

## Architecture

### High-Level Architecture Diagram

```mermaid
graph TB
    subgraph "Data Sources"
        PG[(PostgreSQL<br/>dlq_job table)]
        GCS1[GCS Input 1<br/>CSV Files]
    end
    
    subgraph "Pipeline 1: PostgresToGcsPipeline"
        P1[PostgresToGcsPipeline]
        P1 -->|Queries last hour| PG
        P1 -->|Writes partitioned by hour| GCS_OUT1[GCS Output<br/>dlq/ directory]
    end
    
    subgraph "Pipeline 2: GcsProcessingPipeline"
        P2[GcsProcessingPipeline]
        GCS1 -->|Reads CSV files| P2
        GCS_OUT1 -->|Reads as Input 2| P2
        P2 -->|Writes partitioned output| GCS_OUT2[GCS Output<br/>nodeId_recordType_hour/]
        P2 -->|Writes summary| GCS_SUMMARY[GCS Summary<br/>summary_count/]
    end
    
    style PG fill:#e1f5ff
    style GCS1 fill:#e1f5ff
    style GCS_OUT1 fill:#fff9c4
    style GCS_OUT2 fill:#c8e6c9
    style GCS_SUMMARY fill:#c8e6c9
```

## Pipeline Details

### PostgresToGcsPipeline

Extracts data from a PostgreSQL database table (`dlq_job`) for records from the last hour and writes them to GCS partitioned by hour.

**Purpose**: Export recent DLQ (Dead Letter Queue) job records from PostgreSQL to GCS. The output of this pipeline serves as Input 2 for the GcsProcessingPipeline.

**Input Format**:
- Source: PostgreSQL `dlq_job` table
- Columns: `saga_id`, `node_id`, `create_ts`, `dlq_ts`
- Filter: Records where `dlq_ts >= (current_time - 1 hour)`

**Output Format**:
- Destination: GCS path specified by `--output` parameter
- File naming: `dlq/{hour_timestamp}.csv`
- CSV format: `saga_id,node_id,create_ts,dlq_ts`

#### Low-Level Sequence Diagram: PostgresToGcsPipeline

```mermaid
sequenceDiagram
    participant User
    participant Pipeline as PostgresToGcsPipeline
    participant JdbcIO
    participant PostgreSQL
    participant FileIO
    participant GCS
    
    User->>Pipeline: Execute with options
    Pipeline->>Pipeline: Calculate oneHourAgo timestamp
    Pipeline->>JdbcIO: Read from PostgreSQL
    JdbcIO->>PostgreSQL: Execute query: SELECT saga_id, node_id, create_ts, dlq_ts<br/>FROM dlq_job WHERE dlq_ts >= ?
    PostgreSQL-->>JdbcIO: ResultSet (rows)
    
    loop For each row
        JdbcIO->>JdbcIO: MapRow: Extract fields
        JdbcIO->>JdbcIO: Calculate dlqHour (rounded to hour)
        JdbcIO->>JdbcIO: Format as CSV: saga_id,node_id,create_ts,dlq_ts
        JdbcIO->>JdbcIO: Create KV(dlqHour, csv)
    end
    
    JdbcIO-->>Pipeline: PCollection<KV<String, String>>
    Pipeline->>FileIO: WriteDynamic (partitioned by key)
    
    loop For each unique hour key
        FileIO->>FileIO: Group records by hour
        FileIO->>GCS: Write file: dlq/{hour_timestamp}.csv
    end
    
    FileIO-->>Pipeline: Write complete
    Pipeline-->>User: Pipeline execution complete
```

### GcsProcessingPipeline

Processes CSV files from GCS, extracts and transforms records, performs deduplication, and generates aggregated summaries.

**Purpose**: Process DLQ records from GCS files, deduplicate them, partition by node/type/hour, and generate summary statistics.

**Input Formats**:
1. **Input 1** (required): CSV files with format `sagaIdm,nodeId,createTs` - external GCS source
2. **Input 2** (optional): CSV files with format `sagaIdm,nodeId,createTs,dlqTs` - output from PostgresToGcsPipeline (dlq/ directory)

**Output Formats**:
1. **Main Output**: Partitioned CSV files with naming pattern `{nodeId}_{recordType}_{hour}/part-*.csv`
2. **Summary Output**: Aggregated counts file at `summary_count/summary-*.csv` with format `node_id,countR,countC,countD`

#### Processing Flow

1. **File Matching**: Matches files from GCS based on input patterns
2. **Hour Extraction**: Extracts hour from filename (timestamp-based)
3. **Grouping**: Groups files by hour for processing
4. **Parsing**: Reads and parses CSV records from files
5. **Key Generation**: Creates `OutputKey(nodeId, recordType="D", hour)` for each record
6. **Union**: Combines records from both inputs (if Input 2 is provided)
7. **Deduplication**: Removes duplicate records based on key and value
8. **Dynamic Write**: Writes deduplicated records partitioned by OutputKey
9. **Aggregation**: Counts records per key and generates summary statistics

#### Low-Level Sequence Diagram: GcsProcessingPipeline

```mermaid
sequenceDiagram
    participant User
    participant Pipeline as GcsProcessingPipeline
    participant FileIO
    participant GCS
    participant PCollection as PCollection<Metadata>
    participant GroupByKey
    participant ProcessGroups
    participant Distinct
    participant WriteDynamic
    participant Count
    participant Summary
    
    User->>Pipeline: Execute with options
    Pipeline->>FileIO: MatchFiles: Match input pattern
    FileIO->>GCS: List files matching pattern
    GCS-->>FileIO: File metadata list
    FileIO-->>Pipeline: PCollection<Metadata>
    
    Pipeline->>Pipeline: ExtractHourKey: Extract hour from filename
    loop For each file
        Pipeline->>Pipeline: Parse filename (timestamp) → hour format (yyyy-MM-dd-HH)
        Pipeline->>Pipeline: Create KV(hour, metadata)
    end
    
    Pipeline->>GroupByKey: GroupByHour: Group files by hour
    GroupByKey-->>Pipeline: PCollection<KV<String, Iterable<Metadata>>>
    
    Pipeline->>ProcessGroups: ProcessGroups: Read and parse files
    loop For each file group (by hour)
        loop For each file in group
            ProcessGroups->>GCS: Open and read file
            GCS-->>ProcessGroups: File content (lines)
            loop For each CSV line
                ProcessGroups->>ProcessGroups: Parse: sagaIdm,nodeId,createTs
                ProcessGroups->>ProcessGroups: Extract recordHour from createTs
                ProcessGroups->>ProcessGroups: Create OutputKey(nodeId, "D", recordHour)
                ProcessGroups->>ProcessGroups: Create KV(key, "sagaIdm,createTs")
            end
        end
    end
    ProcessGroups-->>Pipeline: PCollection<KV<OutputKey, String>>
    
    alt Input2 provided
        Pipeline->>Pipeline: Repeat for Input2 (with createTs in value)
        Pipeline->>Pipeline: Union: Combine both PCollections
    end
    
    Pipeline->>Distinct: Deduplicate records
    Distinct-->>Pipeline: PCollection<KV<OutputKey, String>> (unique)
    
    par Main Output
        Pipeline->>WriteDynamic: WriteDynamic: Partition by OutputKey
        WriteDynamic->>GCS: Write files: {nodeId}_{recordType}_{hour}/part-*.csv
    and Summary Output
        Pipeline->>Count: CountPerKey: Count records per OutputKey
        Count-->>Pipeline: PCollection<KV<OutputKey, Long>>
        Pipeline->>Pipeline: PivotForSummary: Group by (nodeId_hour)
        Pipeline->>Summary: FormatSummary: Aggregate counts (R, C, D)
        Summary->>GCS: Write: summary_count/summary-*.csv
    end
    
    Pipeline-->>User: Pipeline execution complete
```

## Configuration

### PostgresToGcsPipeline Options

| Option | Required | Description | Example |
|--------|----------|-------------|---------|
| `--postgresUrl` | Yes | PostgreSQL JDBC connection URL | `jdbc:postgresql://localhost:5432/dbname` |
| `--postgresUser` | Yes | PostgreSQL username | `myuser` |
| `--postgresPassword` | Yes | PostgreSQL password | `mypassword` |
| `--driverClass` | No | JDBC driver class (default: `org.postgresql.Driver`) | `org.postgresql.Driver` |
| `--output` | Yes | GCS output path prefix | `gs://bucket/path/to/output` |

### GcsProcessingPipeline Options

| Option | Required | Description | Example |
|--------|----------|-------------|---------|
| `--input` | Yes | GCS input file pattern | `gs://bucket/input/*.csv` |
| `--input2` | No | Second GCS input file pattern | `gs://bucket/input2/*.csv` |
| `--output` | Yes | GCS output directory | `gs://bucket/output` |

## Building the Project

```bash
mvn clean package
```

This will create a shaded JAR file in the `target/` directory with all dependencies included.

## Running the Pipelines

### PostgresToGcsPipeline

```bash
java -cp target/gcs-processing-pipeline-0.1.0-SNAPSHOT.jar \
  com.example.beam.PostgresToGcsPipeline \
  --postgresUrl=jdbc:postgresql://localhost:5432/mydb \
  --postgresUser=myuser \
  --postgresPassword=mypassword \
  --output=gs://my-bucket/output/prefix
```

### GcsProcessingPipeline

**With single input:**
```bash
java -cp target/gcs-processing-pipeline-0.1.0-SNAPSHOT.jar \
  com.example.beam.GcsProcessingPipeline \
  --input=gs://my-bucket/input/*.csv \
  --output=gs://my-bucket/output
```

**With two inputs:**
```bash
java -cp target/gcs-processing-pipeline-0.1.0-SNAPSHOT.jar \
  com.example.beam.GcsProcessingPipeline \
  --input=gs://my-bucket/input1/*.csv \
  --input2=gs://my-bucket/input2/*.csv \
  --output=gs://my-bucket/output
```

## Running on Dataflow

To run on Google Cloud Dataflow, add the runner option:

```bash
--runner=DataflowRunner \
--project=your-gcp-project \
--region=us-central1 \
--tempLocation=gs://your-bucket/temp
```

## Data Models

### OutputKey

The `OutputKey` class is used in `GcsProcessingPipeline` to partition output records:

- **nodeId**: Identifier for the node
- **recordType**: Type of record (e.g., "D" for DLQ records)
- **hour**: Hour identifier in format `yyyy-MM-dd-HH` (derived from timestamp)

Records are partitioned and written to files named: `{nodeId}_{recordType}_{hour}/part-*.csv`

## Testing

The project includes unit tests and integration tests for both pipelines:

```bash
mvn test
```

Test files:
- `GcsProcessingPipelineTest.java` - Unit tests
- `GcsProcessingPipelineIntegrationTest.java` - Integration tests
- `PostgresToGcsPipelineTest.java` - Unit tests
- `PostgresToGcsPipelineIntegrationTest.java` - Integration tests

## Dependencies

- Apache Beam SDK 2.50.0
- PostgreSQL JDBC Driver 42.6.0
- SLF4J for logging
- JUnit for testing

## License

[Add your license information here]
