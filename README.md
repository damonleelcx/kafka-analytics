How to run

.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
.\bin\windows\kafka-server-start.bat .\config\server.properties
.\bin\windows\kafka-topics.bat --create --topic analytics-topic --bootstrap-server localhost:9092 --partitions 5 --replication-factor 1
.\bin\windows\kafka-topics.bat --delete --topic analytics-topic --bootstrap-server localhost:9092
.\bin\windows\kafka-topics.bat --describe --topic analytics-topic --bootstrap-server localhost:9092

go run cmd/consumer/main.go
go run cmd/producer/main.go

python cmd/producer/multi.py
python cmd/consumer/multi.py
