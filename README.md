# ClickHouse Data Ingestion Tool

A simple tool to transfer data between ClickHouse and CSV files.

## What This Tool Does

- Transfer data from ClickHouse to CSV files
- Transfer data from CSV files to ClickHouse
- Select specific columns for transfer
- Configure connection settings

## How to Run

1. **Start the Backend**
   ```
   cd clickhouse-ingestor
   mvnw.cmd spring-boot:run
   or 
   mvn spring-boot:run

   ```
   (or `./mvnw spring-boot:run` on Linux/Mac)

2. **Access the Application**
   Open your browser and go to:
   ```
   http://localhost:9090
   ```

## Using the Tool

1. **For ClickHouse to CSV:**
   - Select "ClickHouse" as source
   - Fill in connection details (host, port, database, username, JWT token)
   - Select a table
   - Choose "Flat File" as target
   - Set output file name (e.g., `data.csv`)
   - Select columns to transfer
   - Click "Start Ingestion"

2. **For CSV to ClickHouse:**
   - Select "Flat File" as source
   - Choose your CSV file
   - Select "ClickHouse" as target
   - Fill in connection details
   - Select columns to transfer
   - Click "Start Ingestion"

## Requirements

- Java 17 or higher
- Maven
- ClickHouse server (local or cloud)
- JWT token for ClickHouse authentication

## Troubleshooting

If you have issues:
- Check that ClickHouse is running
- Verify your connection details
- Make sure your JWT token is valid
- Check the application logs for errors 
