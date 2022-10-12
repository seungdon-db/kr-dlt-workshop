-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Implement CDC In DLT Pipeline: Change Data Capture
-- MAGIC 
-- MAGIC -----------------
-- MAGIC ###### By Morgan Mazouchi
-- MAGIC -----------------
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/databricks/delta-live-tables-notebooks/main/change-data-capture-example/images/dlt_end_to_end_flow.png">

-- COMMAND ----------

-- MAGIC %python
-- MAGIC slide_id = '10Dmx43aZXzfK9LJvJjH1Bjgwa3uvS2Pk7gVzxhr3H2Q'
-- MAGIC slide_number = 'id.p9'
-- MAGIC  
-- MAGIC displayHTML(f'''<iframe
-- MAGIC  src="https://docs.google.com/presentation/d/{slide_id}/embed?slide={slide_number}&rm=minimal"
-- MAGIC   frameborder="0"
-- MAGIC   width="75%"
-- MAGIC   height="600"
-- MAGIC ></iframe>
-- MAGIC ''')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ##  Change Data Capture (CDC)?
-- MAGIC 
-- MAGIC Change Data Capture (CDC) 는 Database, Data Warehouse와 같은 데이터스토리지에서 생성되는 변경 사항을 기록하는 프로세스입니다. 변경사항에는 데이터 삭제, 추가, 업데이트가 포함됩니다. 
-- MAGIC 
-- MAGIC 데이터 복제에 대한 가장 직관적인 방법은 Database 에 대한 export dump 파일을 받아서 Lakehouse/Datawarehouse/Data Lake에 import하는 방법이지만, 이는 Scalable 한 방법은 아닙니다. 
-- MAGIC 
-- MAGIC Change Data Capture 는 Database에 변경분만 capture해서 이를 target database에 적용하는 방법을 제공합니다. 이를 사용하면 overhead를 줄이고, bulk load updating이 아닌 incremental loading을 통해서 타겟 데이터베익스로 실시간 복제를 가능하게 해줍니다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### CDC Approaches 
-- MAGIC 
-- MAGIC **1- Develop in-house CDC process:** 
-- MAGIC 
-- MAGIC ***Complex Task:*** 데이터베이스간의 차이들, 다양한 레코드 포맷, log record를 읽기 힘든 점 등 때문에 CDC 데이터 복제는 손쉬운 작업이 아닙니다. 
-- MAGIC 
-- MAGIC ***Regular Maintainance:*** CDC 스크립트를 작성해서 프로젝트를 완료했다고 하더라도 변경되는 요구사항들에 대한 유지 보수는 많은 비용과 시간이 소요되는 작업입니다. 
-- MAGIC 
-- MAGIC ***Overburdening:*** 추가 개발 비용
-- MAGIC 
-- MAGIC 
-- MAGIC **2- Using CDC tools** Debezium, Hevo Data, IBM Infosphere, Qlik Replicate, Talend, Oracle GoldenGate, StreamSets 등등
-- MAGIC 
-- MAGIC In this demo repo we are using CDC data coming from a CDC tool. 
-- MAGIC Since a CDC tool is reading database logs:
-- MAGIC We are no longer dependant on developers updating a certain column 
-- MAGIC 
-- MAGIC — A CDC tool like Debezium takes care of capturing every changed row. It records the history of data changes in Kafka logs, from where your application consumes them. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Setup/Requirements:
-- MAGIC 
-- MAGIC  DLT 파이프라인에 1-CDC_DataGenerator notebook 을 attach하도록 하거나, DLT 파이프라인 생성전에 해당 노트북을 수행해서 CDC 데이터를 생성하도록 합니다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SQL Database의 테이블을 Lakehouse로 어떻게 싱크 맞추나? 
-- MAGIC CDC tool, autoloader, DLT pipeline 로 CDC flow 구현하기:
-- MAGIC 
-- MAGIC - A CDC tool 이 database log를 읽어서 row level 변경사항을 json/csv 메세지로 만들어서 kafka 로 전달
-- MAGIC - Kafka 는 row 별  INSERT, UPDATE,DELETE 내용 메세지를 스트리밍해서 cloud object storage (S3 folder, ADLS, etc)에 싱크.
-- MAGIC - Autoloader를 사용해서 cloud storage에 쌓이는 메세지들을 incremental하게 수집해서 bronze table에 저장
-- MAGIC - 1차 정제된 Bronze clean table에 APPLY CHANGES INTO 를 적용해서 변경사항을 silver table 에도 적용 
-- MAGIC 
-- MAGIC 외부 database에서 발생하는 CDC 데이터를 수집하는 도식도입니다. CDC 파일은 kafka 메세지 큐를 포함해 어떠한 포맷도 수용가능합니다. 
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/databricks/delta-live-tables-notebooks/main/change-data-capture-example/images/cdc_flow_new.png" alt='Make all your data ready for BI and ML'/>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Debezium 같은 CDC tool 로그는 어떻게 생겼나?
-- MAGIC 
-- MAGIC 데이터베이스 테이블에 대한 변경사항들에 대한 json 메세지 형태는 다음과 비슷합니다. : 
-- MAGIC 
-- MAGIC - operation: an operation code (DELETE, APPEND, UPDATE, CREATE)
-- MAGIC - operation_date: the date and timestamp for the record came for each operation action
-- MAGIC 
-- MAGIC Some other fields that you may see in Debezium output (not included in this demo):
-- MAGIC - before: the row before the change
-- MAGIC - after: the row after the change
-- MAGIC 
-- MAGIC To learn more about the expected fields check out [this reference](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-update-events)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### Incremental data loading using Auto Loader (cloud_files)
-- MAGIC <div style="float:right">
-- MAGIC   <img width="700px" src="https://raw.githubusercontent.com/databricks/delta-live-tables-notebooks/main/change-data-capture-example/images/DLT_CDC.png"/>
-- MAGIC </div>
-- MAGIC 외부시스템과 연동할때는 schema 변경에 대한 처리가 중요합니다.외부 테이블에서 스키마 업데이트, 컬럼 추가/변경등이 있을 경우 파이프라인의 오류없이 이를 처리할 수 있는 기능이 필요합니다. 
-- MAGIC Databricks Autoloader (`cloudFiles`) 는 스키마 추론 및 스키마 진화(evolution)기능을 제공합니다.
-- MAGIC 
-- MAGIC 또한 Autoloader는 클라우드 스토리지에 있는 수백만개 이상의 파일들에 대한 효율적이고 확장가능한 수집이 가능합니다. 이 노트북에서는 autoloader를 사용해서 스트리밍/배치로 데이터를 처리해 봅시다. 
-- MAGIC 
-- MAGIC Let's use it to create our pipeline and ingest the raw JSON data being delivered by an external provider. 

-- COMMAND ----------

-- DBTITLE 1,Let's explore our incoming data - Bronze Table - Autoloader & DLT
SET
  spark.source;
CREATE
  OR REFRESH STREAMING LIVE TABLE customer_bronze (
    address string,
    email string,
    id string,
    firstname string,
    lastname string,
    operation string,
    operation_date string,
    _rescued_data string
  ) TBLPROPERTIES ("quality" = "bronze") COMMENT "New customer data incrementally ingested from cloud object storage landing zone" AS
SELECT
  *
FROM
  cloud_files(
    "${source}/customers",
    "json",
    map("cloudFiles.inferColumnTypes", "true")
  );

-- COMMAND ----------

-- DBTITLE 1,Silver Layer - Cleansed Table (Impose Constraints)
CREATE OR REFRESH TEMPORARY STREAMING LIVE TABLE customer_bronze_clean_v(
  CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_address EXPECT (address IS NOT NULL),
  CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES ("quality" = "silver")
COMMENT "Cleansed bronze customer view (i.e. what will become Silver)"
AS SELECT * 
FROM STREAM(LIVE.customer_bronze);

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Materializing the silver table
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/databricks/delta-live-tables-notebooks/main/change-data-capture-example/images/cdc_silver_layer.png" alt='Make all your data ready for BI and ML' style='float: right' width='1000'/>
-- MAGIC 
-- MAGIC  `customer_silver` 실버 레이어 테이블은 가장 최신의 업데이트된 뷰를 가지게 됩니다. 이 테이블이 원본 mysql 테이블의 replica가 됩니다. 
-- MAGIC 
-- MAGIC To propagate the `Apply Changes Into` 를 사용해서 다운스트림 `Silver` 레이어까지 변경사항을 적용하기 위해서는 DLT pipeline 설정에서 applyChanges 관련 설정을 넣어서 이 기능을 enable시켜야 합니다. 

-- COMMAND ----------

-- DBTITLE 1,Delete unwanted clients records - Silver Table - DLT SQL 
CREATE OR REFRESH STREAMING LIVE TABLE customer_silver
TBLPROPERTIES ("quality" = "silver")
COMMENT "Clean, merged customers";

-- COMMAND ----------

APPLY CHANGES INTO LIVE.customer_silver
FROM stream(LIVE.customer_bronze_clean_v)
  KEYS (id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY operation_date --auto-incremental ID to identity order of events
  COLUMNS * EXCEPT (operation, operation_date);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 다음으로 DLT 파이프라인을 만들때 applychange 를 true로 바꾸어 주는 설정을 추가합니다. "PipelineSettingConfiguration.json" 파일을 참조하세요.. 
-- MAGIC 
-- MAGIC After running the pipeline, check "3. Retail_DLT_CDC_Monitoring" to monitor log events and lineage data.

-- COMMAND ----------


