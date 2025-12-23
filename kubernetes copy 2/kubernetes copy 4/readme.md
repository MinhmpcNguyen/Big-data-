Ok. Mình sẽ hướng dẫn từ con số 0 theo hướng chuẩn production để sau này bạn “cắm code cũ vào” là chạy được, và rất hợp để lên cloud (EKS/GKE/AKS). Bạn đang làm đúng bài: Kafka + Spark + ES/Kibana trên Kubernetes là combo “data platform starter kit”.

Có 2 cách triển khai Spark trên K8s:
	1.	Spark Standalone trên Kubernetes (có master/worker đúng như bạn nói)

	•	Ưu: nhìn “giống docker-compose” nên dễ hiểu ban đầu
	•	Nhược: production ít dùng; về lâu dài bạn sẽ chuyển sang (2)

	2.	Spark Kubernetes-native (không có master/worker cố định; driver/executor là Pods) ✅ khuyên dùng

	•	Ưu: professional, scale tốt, lên cloud mượt, ít lỗi kiểu bạn vừa gặp
	•	Nhược: lúc đầu hơi lạ vì không có “spark-master container”

Vì bạn muốn “thật chuyên nghiệp” và sau đó làm Bronze/Silver/Gold, mình sẽ hướng dẫn theo (2), nhưng mình cũng chỉ ra nếu bạn vẫn muốn “master-worker” thì làm thế nào.

⸻

A. Thuật ngữ Kafka (để khỏi loạn não)
	•	Broker: 1 server Kafka (trong K8s là 1 pod). Bạn có thể có 3 broker: broker-0,1,2.
	•	Topic: “kênh dữ liệu” (ví dụ weather_raw).
	•	Partition: topic chia nhỏ để song song hóa (vd 6 partitions).
	•	Replication factor: số bản sao mỗi partition (vd 3).
	•	Consumer group: nhóm consumer cùng đọc 1 topic và chia nhau partitions. Spark Structured Streaming dùng group id của nó.

⸻

B. Kiến trúc K8s “chuẩn bài” cho hệ bạn
	•	Kafka: dùng Strimzi Operator (chuẩn industry trên K8s)
	•	Spark: dùng Spark Operator (quản lý SparkApplication CRD)
	•	Elasticsearch + Kibana: dùng ECK Operator (Elastic Cloud on Kubernetes)
	•	S3: bạn đã có code → lát nữa chỉ cần add secret + config s3a:// cho Spark

⸻

C. Bước 0 — Dựng Kubernetes local đúng chuẩn để dev (Kind)

Bạn dùng Mac (arm64) nên Kind là lựa chọn sạch sẽ.

brew install kind kubectl helm
kind create cluster --name data-platform
kubectl cluster-info

Tạo namespace:

kubectl create ns data
kubectl create ns observability


⸻

D. Kafka trên Kubernetes (Strimzi) — từ 0 đến chạy

D1) Cài Strimzi Operator

helm repo add strimzi https://strimzi.io/charts/
helm repo update
helm install strimzi strimzi/strimzi-kafka-operator -n data
kubectl get pods -n data

D2) Tạo Kafka cluster 3 brokers + KRaft/Zookeeper?

Strimzi hiện hỗ trợ KRaft; nhưng để “ăn chắc mặc bền” và dễ debug, nhiều người vẫn dùng ZooKeeper. Mình đưa cấu hình 3 broker + 3 zookeeper trước (rất phổ biến).

Tạo file kafka.yaml:

apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: kafka
  namespace: data
spec:
  kafka:
    version: 3.7.0
    replicas: 3
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
    storage:
      type: ephemeral
    config:
      offsets.topic.replication.factor: 3
      transaction.state.log.replication.factor: 3
      transaction.state.log.min.isr: 2
      min.insync.replicas: 2
      default.replication.factor: 3
  zookeeper:
    replicas: 3
    storage:
      type: ephemeral
  entityOperator:
    topicOperator: {}
    userOperator: {}

Apply:

kubectl apply -f kafka.yaml
kubectl get pods -n data -w

Sau khi lên đủ pod, bạn sẽ có service nội bộ: kafka-kafka-bootstrap.data.svc:9092

D3) Tạo topic weather_raw (6 partitions, replication 3)

topic-weather-raw.yaml:

apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: weather-raw
  namespace: data
  labels:
    strimzi.io/cluster: kafka
spec:
  topicName: weather_raw
  partitions: 6
  replicas: 3
  config:
    retention.ms: 604800000

kubectl apply -f topic-weather-raw.yaml

D4) Test produce/consume nhanh trong cluster

Bạn có thể chạy pod test:

kubectl -n data run kafka-client --restart=Never -it --rm \
  --image=bitnami/kafka:3.7.0 \
  -- bash

Trong pod:

kafka-topics.sh --bootstrap-server kafka-kafka-bootstrap:9092 --list
echo '{"city":"Hanoi","temp":22}' | kafka-console-producer.sh \
  --bootstrap-server kafka-kafka-bootstrap:9092 --topic weather_raw
kafka-console-consumer.sh --bootstrap-server kafka-kafka-bootstrap:9092 \
  --topic weather_raw --from-beginning --max-messages 1


⸻

E. Elasticsearch + Kibana trên K8s (ECK) — chuẩn enterprise

E1) Cài ECK operator

helm repo add elastic https://helm.elastic.co
helm repo update
helm install eck-operator elastic/eck-operator -n observability
kubectl get pods -n observability

E2) Deploy Elasticsearch (1 node dev trước, sau scale)

elasticsearch.yaml:

apiVersion: elasticsearch.k8s.elastic.co/v1
kind: Elasticsearch
metadata:
  name: es
  namespace: observability
spec:
  version: 8.15.0
  nodeSets:
  - name: default
    count: 1
    config:
      node.store.allow_mmap: false

kubectl apply -f elasticsearch.yaml
kubectl get pods -n observability -w

E3) Deploy Kibana

kibana.yaml:

apiVersion: kibana.k8s.elastic.co/v1
kind: Kibana
metadata:
  name: kib
  namespace: observability
spec:
  version: 8.15.0
  count: 1
  elasticsearchRef:
    name: es

kubectl apply -f kibana.yaml
kubectl get pods -n observability -w

E4) Port-forward Kibana để vào UI

kubectl -n observability port-forward svc/kib-kb-http 5601:5601

Mở http://localhost:5601

⸻

F. Spark trên Kubernetes (chuẩn production) — Spark Operator

F1) Cài Spark Operator

helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update
helm install sparkop spark-operator/spark-operator \
  -n data \
  --set webhook.enable=true
kubectl get pods -n data

F2) Tạo ServiceAccount + RBAC cho Spark jobs

spark-rbac.yaml:

apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark
  namespace: data
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-role
  namespace: data
rules:
- apiGroups: [""]
  resources: ["pods","pods/log","services","configmaps"]
  verbs: ["*"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: spark-rolebinding
  namespace: data
subjects:
- kind: ServiceAccount
  name: spark
  namespace: data
roleRef:
  kind: Role
  name: spark-role
  apiGroup: rbac.authorization.k8s.io

kubectl apply -f spark-rbac.yaml


⸻

G. “Spark Streamer” trên K8s (Structured Streaming) — chạy như một SparkApplication

Bạn nói “coi như chưa có code”: vậy ta làm skeleton streamer tối thiểu:
	•	Đọc Kafka topic weather_raw
	•	Parse value dạng string (tạm)
	•	Ghi “debug/bronze” trước: print console hoặc ghi object storage (bỏ qua S3 theo yêu cầu → tạm ghi ra stdout)

G1) Viết code streamer.py (khung tối thiểu)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("weather-streamer").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

bootstrap = "kafka-kafka-bootstrap.data.svc:9092"
topic = "weather_raw"

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", bootstrap)
    .option("subscribe", topic)
    .option("startingOffsets", "latest")
    .load()
)

out = df.select(col("timestamp"), col("topic"),
                col("partition"), col("offset"),
                col("value").cast("string").alias("value"))

q = (
    out.writeStream
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "/tmp/chk/weather_streamer")
    .start()
)

q.awaitTermination()

G2) Đóng gói image (multi-arch) — cực quan trọng cho “mọi máy”

Bạn nên build bằng buildx:

docker buildx create --use
docker buildx build --platform linux/amd64,linux/arm64 \
  -t YOUR_DOCKERHUB/weather-spark:dev \
  --push .

Dockerfile mẫu:

FROM bitnami/spark:3.5.1
USER root
COPY streamer.py /app/streamer.py
USER 1001

G3) Tạo SparkApplication CRD cho streamer

spark-streamer.yaml:

apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: weather-streamer
  namespace: data
spec:
  type: Python
  mode: cluster
  image: YOUR_DOCKERHUB/weather-spark:dev
  imagePullPolicy: Always
  mainApplicationFile: local:///app/streamer.py
  sparkVersion: 3.5.1
  restartPolicy:
    type: Always
  driver:
    serviceAccount: spark
    cores: 1
    coreLimit: "1"
    memory: "1g"
  executor:
    instances: 2
    cores: 1
    memory: "1g"
  deps:
    packages:
      - org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1

Apply:

kubectl apply -f spark-streamer.yaml
kubectl get sparkapplications -n data
kubectl get pods -n data -w

Xem log driver:

kubectl -n data logs -f pod/weather-streamer-driver


⸻

H. Spark Batch job trên K8s (chuẩn “daily/hourly”) — 2 cách

Cách professional nhất là dùng CronJob để apply/trigger SparkApplication, hoặc dùng workflow engine (Argo).
Trước mắt “từ đầu” dễ nhất:

H1) Viết batch skeleton batch.py

Ví dụ: đọc “bronze” → transform → ghi “silver” (tạm thời chỉ log ra)

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("weather-batch").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# TODO: sau này đọc S3/bronze
df = spark.createDataFrame([("Hanoi", 22), ("HCM", 31)], ["city", "temp"])
df.show(truncate=False)

H2) SparkApplication cho batch (run once)

apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: weather-batch-once
  namespace: data
spec:
  type: Python
  mode: cluster
  image: YOUR_DOCKERHUB/weather-spark:dev
  imagePullPolicy: Always
  mainApplicationFile: local:///app/batch.py
  sparkVersion: 3.5.1
  restartPolicy:
    type: Never
  driver:
    serviceAccount: spark
    cores: 1
    memory: "1g"
  executor:
    instances: 2
    cores: 1
    memory: "1g"


⸻

I. Nếu bạn “cố tình muốn spark master-worker” trên K8s thì sao?

Có thể, nhưng mình coi đó là “phase 0 để học”, không phải đích đến.
	•	Deploy spark-master + spark-worker StatefulSet/Deployment
	•	Streamer/batch submit vào spark://spark-master:7077

Nhưng: bạn sẽ lại gặp các vấn đề networking/RPC/arch mà K8s-native giải quyết sạch hơn. Vì bạn nhắm production, mình khuyên bỏ nhánh này.

⸻

J. Checklist chạy được “mọi máy” (Mac/Win/Linux) và dễ lên cloud
	1.	Tất cả image tự build phải multi-arch (amd64 + arm64)
	2.	Kafka dùng Strimzi, ES dùng ECK, Spark dùng Spark Operator
	3.	Không hardcode kafka-1:9092 nữa → dùng service DNS:
	•	kafka-kafka-bootstrap.data.svc:9092
	4.	Mọi config/secret để qua:
	•	ConfigMap (config)
	•	Secret (key, password, S3 creds)
	5.	Chạy local bằng Kind → y chang production

⸻

K. “Sau đó hợp nhất với code cũ” sẽ làm thế nào?

Khi bạn đã dựng xong platform như trên, bước hợp nhất chỉ là:
	•	Đưa code streamer cũ vào /app/streamer.py
	•	Đưa code batch cũ vào /app/batch.py
	•	Thêm S3 secret + spark config spark.hadoop.fs.s3a.*
	•	Thêm checkpointLocation về S3 (hoặc PVC) để streaming “chuẩn bài”
	•	Sau đó mới bắt đầu làm Bronze/Silver/Gold:
	•	Bronze: raw JSON từ Kafka → Parquet/Delta/Iceberg trên S3
	•	Silver: cleaned schema + dedup
	•	Gold: aggregates phục vụ dashboard / search

⸻

Nếu bạn làm đúng các bước trên, hệ của bạn sẽ chuyển hẳn từ “chạy được” sang platform đúng chuẩn để triển khai thật.

Nếu bạn muốn mình “đẩy tới luôn” ngay trong lượt này: mình có thể viết luôn bộ file YAML/Helm values theo cấu trúc repo kiểu:

infra/
  kafka/
  spark/
  elastic/
apps/
  streamer/
  batch/

và đưa luôn lệnh chạy theo thứ tự để bạn copy-paste.

kubectl get sparkapp
kubectl get pods
kubectl get pvc

kubectl delete pvc spark-checkpoint-pvc
kubectl delete sparkapp weather-streaming
kubectl delete sparkapplication weather-streaming -n default 
kubectl apply -f k8s/spark-streaming.yaml

docker build -t spark-weather-kafka:3.5.1 .
kind load docker-image spark-weather-kafka:3.5.1 --name data-platform
kubectl delete sparkapp weather-streaming
kubectl apply -f k8s/spark-streaming.yaml



# RBAC + PVC + SparkApplication
kubectl apply -f k8s/sa-rbac.yaml
kubectl apply -f k8s/checkpoint-pvc.yaml
kubectl apply -f k8s/spark-streaming.yaml

kubectl get pods -n default -l spark-role=driver -w

kubectl describe sparkapplication weather-streaming -n default
kubectl logs -n default -l spark-role=driver -f

kubectl apply -f k8s/elasticsearch.yaml
kubectl logs -n default weather-streaming-driver -f

kubectl exec -n data -it kafka-kafka-pool-0 -c kafka -- bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list

  docker exec -it data-platform-control-plane crictl images
  docker exec -it data-platform-control-plane crictl rmi spark-weather-kafka:3.5.1-r1
  docker build -t spark-weather-kafka:3.5.1 .
kind load docker-image spark-weather-kafka:3.5.1 --name data-platform
docker exec -it data-platform-control-plane crictl rmi spark-weather-kafka:3.5.1-r2
docker exec -it data-platform-control-plane crictl rmi spark-weather-kafka:3.5.1-r3

docker exec -it data-platform-control-plane bash
# Xem dung lượng
df -h

# Dọn snapshot containerd
ctr -n k8s.io snapshots list
ctr -n k8s.io snapshots prune

# Dọn content (blobs)
ctr -n k8s.io content prune
crictl rmi docker.io/library/spark-weather-kafka:3.5.1
crictl images | grep spark-weather
exit

kubectl describe pod weather-streaming-driver

kubectl logs bronze-to-es-batch-driver

kubectl exec -it kafka-kafka-pool-0 -n data -- bash
/opt/kafka/bin/kafka-console-producer.sh \
--bootstrap-server kafka-kafka-bootstrap.data.svc.cluster.local:9092 \
--topic weather-raw

/opt/kafka/bin/kafka-console-consumer.sh \
--bootstrap-server kafka-kafka-bootstrap.data.svc.cluster.local:9092 \
--topic weather-raw \
--from-beginning

/opt/kafka/bin/kafka-console-consumer.sh \
--bootstrap-server kafka-kafka-bootstrap.data.svc.cluster.local:9092 \
--topic weather-raw \
--from-beginning \
--timeout-ms 10000

kubectl exec -n data kafka-kafka-pool-0 -- \
/opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
--bootstrap-server kafka-kafka-bootstrap.data.svc.cluster.local:9092 \
--topic weather-raw


kubectl apply -f kafka.yaml
kubectl get pods -n data





docker exec -it data-platform-control-plane crictl rmi spark-weather-stream:3.3.3-r6
docker build -t spark-weather-stream:3.3.3-r6 .    
kind load docker-image spark-weather-stream:3.3.3-r6 --name data-platform


kubectl delete sparkapplication weather-streaming-to-bronze -n default
kubectl delete sparkapplication bronze-to-es-batch -n default

kubectl apply -f k8s/weather-streaming-to-bronze.yaml
kubectl logs -n default weather-streaming-to-bronze-driver --tail=200
kubectl logs -n default weather-streaming-to-bronze-driver -f

kubectl apply -f k8s/bronze-to-es-batch.yaml

kubectl exec -n default weather-streaming-to-bronze-driver -- \
ls /checkpoint/bronze_parquet/_spark_metadata  

kubectl logs -n default -l spark-role=driver --tail=200
kubectl exec -n observability es-weather-es-default-0 -- \
curl -u elastic:3kc9mR6Ke158uN4bXE8Rz17a \
"http://localhost:9200/_cat/indices?v"




KAFKA PRODUCER
docker exec -it data-platform-control-plane crictl rmi weather-kafka-producer:latest

docker build -t weather-kafka-producer:latest .   
kind load docker-image weather-kafka-producer:latest --name data-platform
kubectl create secret generic openweather-secret \
  --from-literal=api-key=d494b1a1af4978a9adbbe5eb9e39698b \
  -n data


  kubectl get pods -n data -w
  kubectl logs -n data deploy/weather-kafka-producer

Mở rộng thêm kafka
kubectl delete pod weather-streaming-to-bronze-driver
kubectl get sparkapplication -n default
kubectl apply -f weather-producer.yaml
kubectl rollout restart deployment weather-kafka-producer -n data

kubectl delete pod -l app=weather-kafka-producer

kubectl get pods | grep weather-kafka-producer

kubectl get sparkapplication
kubectl get pods | grep weather-streaming

kubectl exec -it -n data kafka-kafka-pool-0 -- bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-kafka-bootstrap.data.svc.cluster.local:9092 \
  --topic weather-raw

kubectl exec -it silver-debug -- rm -rf /checkpoint/bronze*
kubectl exec -it silver-debug -- rm -rf /checkpoint/gold_state
và đổi index

kubectl exec -it silver-debug -- rm -rf /data/silver/weather (optional)



Stream

kubectl delete sparkapplication weather-streaming-to-bronze -n default
kubectl exec -it checkpoint-debug -- sh
ls -lh /checkpoint
rm -rf /checkpoint/weather-raw


docker exec -it data-platform-control-plane crictl rmi spark-weather-stream:3.3.3-r8


docker build -t spark-weather-stream:3.3.3-r8 .    
kind load docker-image spark-weather-stream:3.3.3-r8 --name data-platform


kubectl apply -f k8s/weather-streaming-to-bronze.yaml
kubectl logs -n default weather-streaming-to-bronze-driver --tail=200
kubectl logs -n default weather-streaming-to-bronze-driver -f


bronze ES
kubectl delete cronjob bronze-to-es-batch-cron
kubectl delete job -l job-name=bronze-to-es

kubectl exec -it checkpoint-debug -- sh
ls -lh /checkpoint
rm -rf /checkpoint/batch_state

kubectl delete cronjob bronze-to-es-batch
kubectl delete job -l job-name=bronze-to-es

kubectl delete sparkapplication weather-streaming-to-bronze
kubectl exec -it checkpoint-debug -- rm -rf /checkpoint/weather-raw/bronze
kubectl exec -it checkpoint-debug -- sh
ls -lh /checkpoint
rm -rf /checkpoint/bronze_parquet
docker exec -it data-platform-control-plane crictl rmi spark-weather-bronze:3.3.3-r6


docker build -t spark-weather-bronze:3.3.3-r6 .    
kind load docker-image spark-weather-bronze:3.3.3-r6 --name data-platform
kubectl apply -f k8s/weather-streaming-to-bronze.yaml

kubectl apply -f k8s/bronze-to-es-batch.yaml

kubectl create job bronze-to-es-manual \
  --from=cronjob/bronze-to-es-batch-cron
kubectl get sparkapplication
kubectl get sparkapplication weather-streaming-to-bronze
# Bronze parquet có full schema
kubectl exec -it checkpoint-debug -- \
  spark-shell -e "spark.read.parquet(\"/checkpoint/bronze_parquet\").printSchema()"

# Batch job status
kubectl get sparkapplication | grep bronze-to-es-batch
kubectl delete sparkapplication bronze-to-es-batch-46pf8 \
  bronze-to-es-batch-nr2qv \
  bronze-to-es-batch-qhktq \
  bronze-to-es-batch-rdpm6 \
  bronze-to-es-batch-vpd25 \
  bronze-to-es-batch-vzvlt
kubectl describe sparkapplication bronze-to-es-batch-qhktq
kubectl logs -f bronze-to-es-batch-qhktq-driver

kubectl exec -it checkpoint-debug -- ls -lh /checkpoint/bronze_parquet
kubectl exec -it checkpoint-debug -- \
  find /checkpoint/bronze_parquet -type f | wc -l
kubectl logs job/bronze-to-es-manual
kubectl get jobs


kubectl get pods --selector=job-name=bronze-to-es-manual
kubectl logs bronze-to-es-manual-f56wg

kubectl exec -n default weather-streaming-to-bronze-driver -- \
ls /checkpoint/bronze_parquet/_spark_metadata  

S3 

docker build -t weather-s3-history:latest .
kind load docker-image weather-s3-history:latest --name data-platform

docker exec -it data-platform-control-plane crictl rmi weather-s3-history:latest
kubectl create secret generic aws-secret \
  -n data \
  --from-literal=access-key=AKIAWR7VUBKMFR6UKKNR \
  --from-literal=secret-key=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

kubectl delete cronjob weather-history-to-s3 -n data
kubectl delete job -n data -l job-name=weather-history-to-s3
kubectl apply -f weather-history-cronjob.yaml



 kubectl create job \
  --from=cronjob/weather-history-to-s3 \
  weather-history-manual \
  -n data
kubectl get pods -n data -w
kubectl logs -n data weather-history-manual-d2rvq  
kubectl logs -n data weather-history-to-s3-29436765-cqbdj

Xoá parquet S3
aws s3 ls s3://hust-bucket-storage/weather_silver/
aws s3 rm s3://hust-bucket-storage/weather_silver/ --recursive
aws s3 ls s3://hust-bucket-storage/weather_silver/

aws s3 cp s3://hust-bucket-storage/weather_silver/Hanoi.parquet .
python - << 'EOF'
import pyarrow.parquet as pq
print(pq.read_table("Hanoi.parquet").schema)
EOF

Silver
docker exec -it data-platform-control-plane crictl rmi spark-weather-silver:3.3.3
docker build -t spark-weather-silver:3.3.3 .
kind load docker-image spark-weather-silver:3.3.3 --name data-platform
kubectl apply -f silver-pvc.yaml
kubectl apply -f silver-from-s3.yaml


kubectl delete sparkapplication weather-s3-to-silver -n default
kubectl delete pvc spark-silver-pvc -n default
kubectl apply -f silver-from-s3.yaml
kubectl get pods -n default -w

kubectl create secret generic aws-credentials \
  --from-literal=AWS_ACCESS_KEY_ID=AKIAWR7VUBKMFR6UKKNR \
  --from-literal=AWS_SECRET_ACCESS_KEY=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 \
  -n default

kubectl logs -n default weather-s3-to-silver-driver -f


elastic
kubectl get elasticsearch -n observability
kubectl get pods -n observability | grep es-weather
kubectl get svc -n observability | grep es-weather
kubectl logs -n observability es-weather-es-default-0
kubectl port-forward -n observability svc/es-weather-es-http 9200:9200

kibana
kubectl apply -f kibana.yaml
kubectl get kibana -n observability -w
kubectl get pods -n observability | grep kibana
kubectl port-forward -n observability svc/kibana-weather-kb-http 5601:5601


gold
docker exec -it data-platform-control-plane crictl rmi spark-weather-gold:3.3.3
docker build -t spark-weather-gold:3.3.3 .
kind load docker-image spark-weather-gold:3.3.3 --name data-platform
docker images | grep spark-weather-gold

kubectl apply -f gold-pvc.yaml
kubectl get pvc | grep gold

kubectl delete sparkapplication silver-to-gold-es --ignore-not-found
kubectl apply -f silver-to-gold-es.yaml
kubectl get pods -n default -w

nếu muốn đổi index
kubectl apply -f checkpoint-debug.yaml

kubectl exec -it checkpoint-debug -- sh
ls -lh /checkpoint
rm -rf /checkpoint/gold_state
ls -lh /checkpoint

 Kiểm tra dữ liệu Gold (parquet)
kubectl exec -it silver-debug -- bash
ls -lh /data/gold/weather_hourly
spark-shell

val df = spark.read.parquet("/data/gold/weather_hourly")
df.printSchema()
df.show(20, false)


ML
docker exec -it data-platform-control-plane crictl rmi spark-weather-ml:3.3.3-v1
kubectl delete cronjob weather-ml-inference-cron --ignore-not-found
kubectl delete job weather-ml-inference-manual --ignore-not-found

docker build -t spark-weather-ml:3.3.3-v1 .
docker run --rm spark-weather-ml:3.3.3-v1 ls -lh /models

kind load docker-image spark-weather-ml:3.3.3-v1 --name data-platform

kubectl get sparkapplication

kubectl get pods | grep weather-ml-inference




kubectl delete sparkapplication weather-ml-inference-2qn5j


kubectl get pods | grep weather-ml-inference

kubectl apply -f ml.yaml

kubectl create job weather-ml-inference-manual \
  --from=cronjob/weather-ml-inference-cron

kubectl get jobs | grep weather-ml
kubectl get pods | grep weather-ml-inference

kubectl logs -f weather-ml-inference-7dfg7-driver
kubectl describe pod weather-ml-inference-vdwnz-driver


kubectl get pods -l spark-app-name=weather-ml-inference-b9wc8

kubectl delete cronjob weather-ml-inference-cron


kubectl get cronjob weather-ml-inference-cron
kubectl get pods -w | grep weather-ml

kubectl get jobs
kubectl get sparkapplication

kubectl get pods | grep weather-ml-inference
kubectl describe sparkapplication <app-name> | grep "Pod Name"
kubectl logs -f weather-ml-inference-7dfg7-driver


kubectl get pods -l spark-role=driver | grep weather-ml-inference
kubectl describe sparkapplication weather-ml-inference-2qn5j
kubectl describe pod weather-ml-inference-vdwnz-driver


delete bronze batch
kubectl get sparkapplication | grep bronze-to-es-batch
kubectl delete sparkapplication $(kubectl get sparkapplication | grep bronze-to-es-batch | awk '{print $1}')

kubectl apply -f k8s/bronze-to-es-batch.yaml


kiểm tra CPU
kubectl describe node data-platform-control-plane | egrep -A3 "Allocatable|Allocated resources|memory|cpu"

kubectl get pods --field-selector=status.phase=Running \
  -o custom-columns="NAME:.metadata.name,MEM_REQ:.spec.containers[*].resources.requests.memory" \
  | sort -k2

  kubectl get pods -l spark-role=executor \
  -o custom-columns="POD:.metadata.name,APP:.metadata.labels.spark-app-name,MEM:.spec.containers[*].resources.requests.memory"