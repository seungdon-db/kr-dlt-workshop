-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Simplify ETL with Delta Live Table
-- MAGIC 
-- MAGIC DLT를 써서 모든 사람이 데이터 엔지니어링 작업을 해봅시다! SQL이나 Python으로 데이터의 변경작업을 선언하기만 하면 DLT가  데이터 엔지니어링하는데 필요한 복잡한 작업들을 대신 해줄 거에요. 
-- MAGIC 
-- MAGIC <img style="float:right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-1.png" width="700"/>
-- MAGIC 
-- MAGIC **ETL 개발 가속화** <br/>
-- MAGIC 분석가나 데이터엔지니어가 간단하게 파이프라인을 만들고 운영하게 도와줍니다. 
-- MAGIC 
-- MAGIC **운영 복잡성 제거** <br/>
-- MAGIC 복잡한 운영 작업들을 자동화하고 파이프라인 운영의 가시성을 제공합니다. 
-- MAGIC 
-- MAGIC **너의 데이터를 믿으세요** <br/>
-- MAGIC 빌트인 품질 관리와 모니터링 기능으로 BI,DS,ML 시에 사용될 데이터에 대한 정확성을 확보하세요.
-- MAGIC 
-- MAGIC **배치와 스트리밍 간편화** <br/>
-- MAGIC 자동 최적화되고 오토스케일링 데이터 파이프 라인으로 배치나 스트리밍을 한꺼번에 처리하세요. 
-- MAGIC 
-- MAGIC ## Our Delta Live Table pipeline
-- MAGIC 
-- MAGIC 우리는 실시간 고객 대출 정보와 과거 대출 정보를 기반으로 이 데이터를 준실시간 수집하고 가공해서 분석가에게 제공하고자 합니다. 

-- COMMAND ----------

-- MAGIC %md-sandbox 
-- MAGIC 
-- MAGIC ## Bronze layer: incrementally ingest data leveraging Databricks Autoloader
-- MAGIC 
-- MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-2.png" width="500"/>
-- MAGIC 
-- MAGIC Raw Data가 Cloud Storage에 적재되면,
-- MAGIC 
-- MAGIC Autoloader 는 스키마 참조 및 스키마 변경를 포함해서 수백만개의 파일들에 대해서 간단하게 이를 수집합니다.
-- MAGIC 
-- MAGIC DLT에서는 Autoloader 를 SQL상에서  `cloud_files` 함수를 써서 다양한 포맷(json, csv, avro...) 데이터를 수집할 수 있습니다.:
-- MAGIC 
-- MAGIC 
-- MAGIC #### STREAMING LIVE TABLE 
-- MAGIC 테이블을  `STREAMING` 타입으로 선언하면 새로 들어오는 데이터에 대해서만 수집을 보장합니다. 
-- MAGIC 1. Kafka, Kinesis, Autoloader (files on cloud storage) 와 같은 append-only 스트리밍 데이터 
-- MAGIC 
-- MAGIC 2. Delta tables with delta.appendOnly=true
-- MAGIC 
-- MAGIC 3. Produce results on demand: 
-- MAGIC 
-- MAGIC   - Lower latency: more frequent less data processing
-- MAGIC   - Lower costs: by avoiding redundant reprocessing
-- MAGIC     
-- MAGIC     
-- MAGIC #### Data sources:
-- MAGIC 
-- MAGIC 이 핸즈온 랩에서는 2개의 데이터 소스를 사용합니다. 
-- MAGIC 하나는  "/databricks-datasets/lending-club-loan-stats/LoanStats_*" 에 있는 CSV 데이터파일들과  
-- MAGIC 다른 하나는 faker library를 사용해서 우리가 지정한 경로에 쌓이고 있는 json데이터 파일들입니다.
-- MAGIC 두개의 파일 경로에 대해서 pipeline을 생성할떄 confiration 을 넘겨줘야 합니다.   
-- MAGIC 다음과 같이 설정합니다. 
-- MAGIC 
-- MAGIC          "loanStats": "/databricks-datasets/lending-club-loan-stats/LoanStats_*"
-- MAGIC          "input_data": "/home/techsummit/dlt"

-- COMMAND ----------

-- DBTITLE 1,Read Raw Data in DLT via Autoloader (Table 1: raw_txs) with Parametrization
CREATE STREAMING LIVE TABLE raw_txs    
COMMENT "New raw loan data incrementally ingested from cloud object storage landing zone"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT * FROM cloud_files('${input_data}/landing', 'json', map("cloudFiles.schemaEvolutionMode", "rescue"));

-- COMMAND ----------

-- DBTITLE 1,Read Raw Data in DLT via Autoloader (Table 2: reference_loan_stats) + Optimize Data Layout for Performance
CREATE STREAMING LIVE TABLE reference_loan_stats
COMMENT "Raw historical transactions"
TBLPROPERTIES --Can be spark, delta, or DLT confs
("quality"="bronze",
"pipelines.autoOptimize.managed"="true",
"pipelines.autoOptimize.zOrderCols"="CustomerID, InvoiceNo",
"pipelines.trigger.interval"="1 hour"
 )
AS SELECT * FROM cloud_files('${loanStats}', 'csv', map("cloudFiles.inferColumnTypes", "true")) -- loanStats: /databricks-datasets/lending-club-loan-stats/LoanStats_*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### LIVE TABLES 
-- MAGIC 
-- MAGIC - 가능한 모든 데이터를 한번에 읽어서 수집힙니다. 
-- MAGIC - Always "correct", meaning their contents will match their definition after any update.
-- MAGIC 
-- MAGIC - Today they are computed completely, but we are going to get much more clever as time goes on (go/enzyme) . 

-- COMMAND ----------

-- DBTITLE 1,Read Raw Data Stored in Delta Tables in DLT (Table 3: ref_accounting_treatment) with Parametrization
CREATE LIVE TABLE ref_accounting_treatment    
COMMENT "Lookup mapping for accounting codes"
AS SELECT * FROM delta.`${input_data}/ref_accounting_treatment/` ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Silver layer: joining tables while ensuring data quality
-- MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-3.png" width="500"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 브론즈 레이어가 정의되었으니 이를 조인해서 실버 레이어를 만들도록 하겠습니다. 브론즈 테이블들에 대해서 참조할때 `LIVE` spacename을 사용하는 것을 주목해 주세요. 
-- MAGIC 
-- MAGIC 브론즈 레이어에서 `raw_txs` 테이블과 같이 increment하게 수집된 부분만 처리하기 위해서는 `stream` 키워드를 사용할 것이니다: `stream(LIVE.raw_txs)`
-- MAGIC 
-- MAGIC 파일 압축등은 걱정하지 마세요. DLT가 알아서 처리해 줄 겁니다.
-- MAGIC 
-- MAGIC 
-- MAGIC #### Expectations
-- MAGIC 
-- MAGIC DLT 는 현재 3가지 모드의 Expectation을 지원합니다. :
-- MAGIC 
-- MAGIC | Mode | Behavior |
-- MAGIC | ---- | -------- |
-- MAGIC | `EXPECT(criteria)` in SQL or `@dlt.expect` in Python  | Expecation을 위반하는 건수들의 전체 대비 비율(%)메트릭 기록<br> (**NOTE**: 이 메트릭은 모든 execution mode에서 리포팅 됩니다. |
-- MAGIC | `EXPECT (criteria) ON VIOLATION FAIL UPDATE` in SQL or `@dlt.expect_or_fail` in Python| Expectation이 충족되지 않으면 pipeline이 실패합니다. |
-- MAGIC | `EXPECT (criteria) ON VIOLATION DROP ROW` in SQL or `@dlt.expect_or_drop` in Python| Expectation이 충족되는 레코드만 처리합니다. |
-- MAGIC 
-- MAGIC Expectation을 선언하면  (`CONSTRAINT <name> EXPECT <condition>`), 데이터 품질 지표에 대해서 강제하고 이를 트래킹할 수 있습니다. . 상세 문서 [documentation](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-expectations.html)를 참고하세요

-- COMMAND ----------

-- DBTITLE 1,Perform ETL & Enforce Quality Expectations
CREATE STREAMING LIVE TABLE cleaned_new_txs (
  CONSTRAINT `Payments should be this year`  EXPECT (next_payment_date > date('2021-12-31')) ON VIOLATION DROP ROW, 
  CONSTRAINT `Balance should be positive`    EXPECT (balance > 0 AND arrears_balance > 0) ON VIOLATION DROP ROW,    
  CONSTRAINT `Cost center must be specified` EXPECT (cost_center_code IS NOT NULL) ON VIOLATION FAIL UPDATE         
)
COMMENT "Livestream of new transactions, cleaned and compliant"
TBLPROPERTIES ("quality" = "silver")
AS SELECT txs.*, rat.id as accounting_treatment FROM stream(LIVE.raw_txs) txs
INNER JOIN live.ref_accounting_treatment rat ON txs.accounting_treatment_id = rat.id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Expectation을 정의해서 데이터 품질 정의에 맞지 않는 데이터를 필터링하였지만, 잘못된 데이터도 따로 저장해서 분석에 활용하고 싶습니다.   
-- MAGIC 선언한 Expectation에 반대되는 Expectation rule을 만들고 이를 별도의 테이블에 저장하도록 합니다. 

-- COMMAND ----------

-- DBTITLE 1,Quarantine Invalid Data with Expectations
CREATE STREAMING LIVE TABLE quarantined_cleaned_new_txs
(
  CONSTRAINT `dropped rows` EXPECT ((next_payment_date <= '2021-12-31') OR (balance <= 0 OR arrears_balance <= 0) OR (cost_center_code IS NULL)) ON VIOLATION DROP ROW
)
COMMENT "Livestream of quarantined invalid records"
TBLPROPERTIES 
("quality"="silver")
AS SELECT txs.*, rat.id as accounting_treatment FROM stream(LIVE.raw_txs) txs
INNER JOIN live.ref_accounting_treatment rat ON txs.accounting_treatment_id = rat.id;

-- COMMAND ----------

-- DBTITLE 1,Historical Loan Transactions
CREATE LIVE TABLE historical_txs
COMMENT "Historical loan transactions"
TBLPROPERTIES ("quality" = "silver")
AS SELECT a.* FROM LIVE.reference_loan_stats a
INNER JOIN LIVE.ref_accounting_treatment b USING (id);

-- COMMAND ----------

-- MAGIC %md-sandbox 
-- MAGIC 
-- MAGIC ## Gold layer
-- MAGIC 
-- MAGIC 
-- MAGIC 마지막 단계로 골드 레이어에서 사용자들이 직접 조회할 데이터 테이블 들을 생성합니다. 
-- MAGIC 
-- MAGIC SQL Warehouse를 사용해서 여러 사용자들이 조회할 테이블이니 테이블레벨에서 `pipelines.autoOptimize.zOrderCols` 를 사용해서 빠른 쿼리 성능을 보장하도록 합니다.
-- MAGIC 
-- MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-4.png" width="500"/>

-- COMMAND ----------

CREATE LIVE TABLE total_loan_balances_1
COMMENT "Combines historical and new loan data for unified rollup of loan balances"
TBLPROPERTIES (
  "quality" = "gold",
  "pipelines.autoOptimize.zOrderCols" = "location_code"
)
AS SELECT sum(revol_bal)  AS bal,
addr_state   AS location_code 
FROM live.historical_txs  GROUP BY addr_state
UNION SELECT sum(balance) AS bal, 
country_code AS location_code 
FROM live.cleaned_new_txs GROUP BY country_code;

-- COMMAND ----------

CREATE LIVE TABLE total_loan_balances_2
COMMENT "Combines historical and new loan data for unified rollup of loan balances"
TBLPROPERTIES ("quality" = "gold")
AS SELECT sum(revol_bal)  AS bal,
addr_state   AS location_code 
FROM live.historical_txs  GROUP BY addr_state
UNION SELECT sum(balance) AS bal,
country_code AS location_code 
FROM live.cleaned_new_txs GROUP BY country_code;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Live Views
-- MAGIC 
-- MAGIC 뷰는 live table과 동일한 데이터 업데이트 보장을 지원하지만, 테이블과 달리 쿼리 결과가 disk에 저장되지 않습니다. 
-- MAGIC Databricks 의 다른 view들과 달리 DLT views 는  metastore에 저장되지 않아서 오직 DLT pipeline내에서만 참조할 수 있습니다. 
-- MAGIC 
-- MAGIC - Views are virtual, more like a name for a query
-- MAGIC 
-- MAGIC - Use views to break up large/complex queries
-- MAGIC 
-- MAGIC - Expectations on views validate correctness of intermediate results (data quality metrics for views are not designed to be observable in UI)
-- MAGIC 
-- MAGIC - Views are recomputed every time they are required

-- COMMAND ----------

CREATE LIVE VIEW new_loan_balances_by_cost_center
COMMENT "Live view of new loan balances for consumption by different cost centers"
TBLPROPERTIES (
  "quality" = "gold",
  "pipelines.autoOptimize.zOrderCols" = "cost_center_code"
)
AS SELECT sum(balance), cost_center_code
FROM live.cleaned_new_txs
GROUP BY cost_center_code;

-- COMMAND ----------

CREATE LIVE VIEW new_loan_balances_by_country     -- TO DO --
COMMENT "Live view of new loan balances per country"
TBLPROPERTIES (
  "quality" = "gold",
  "pipelines.autoOptimize.zOrderCols" = "country_code"
)
AS SELECT sum(count), country_code
FROM live.cleaned_new_txs
GROUP BY country_code;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ## Now it is time to create a pipeline!
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Navigate to the workflows in the menue, switch to delta live tables and click on create a pipeline! 
-- MAGIC 
-- MAGIC * Note you need to add data location paths as two parameters in your pipeline configuration:
-- MAGIC 
-- MAGIC       - key: loanStats , value: /databricks-datasets/lending-club-loan-stats/LoanStats_*
-- MAGIC       - key: input_data, value: /home/techsummit/dlt

-- COMMAND ----------

-- MAGIC %md ## Tracking data quality
-- MAGIC 
-- MAGIC Expectations stats are automatically available as system table.
-- MAGIC 
-- MAGIC This information let you monitor your data ingestion quality. 
-- MAGIC 
-- MAGIC You can leverage DBSQL to request these table and build custom alerts based on the metrics your business is tracking.
-- MAGIC 
-- MAGIC 
-- MAGIC See [how to access your DLT metrics]($./03-Log-Analysis)
-- MAGIC 
-- MAGIC <img width="500" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dlt-data-quality-dashboard.png">
-- MAGIC 
-- MAGIC <a href="https://e2-demo-field-eng.cloud.databricks.com/sql/dashboards/6f73dd1b-17b1-49d0-9a11-b3772a2c3357-dlt---retail-data-quality-stats?o=1444828305810485" target="_blank">Data Quality Dashboard example</a>
