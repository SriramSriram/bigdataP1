
@echo off
echo ========================================
echo    BigDataP1 Pipeline Setup
echo ========================================

echo Starting Docker containers...
docker-compose up -d

echo Waiting for Kafka to start...
timeout /t 30

echo Creating Kafka topics...
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic kafka-energy-raw --partitions 1 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic kafka-energy-clean --partitions 1 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic kafka-energy-alerts --partitions 1 --replication-factor 1 --if-not-exists

echo Installing Python dependencies...
pip install requests kafka-python pyspark pandas flask --quiet

echo ========================================
echo Setup Complete! 
echo.
echo Open these in your browser:
echo n8n       : http://localhost:5678
echo Kafka UI  : http://localhost:8085
echo Jenkins   : http://localhost:8090
echo Jupyter   : http://localhost:8888
echo ========================================

echo Starting Pipeline API...
python api/pipeline_api.py