# ConsultantPlus Session Log Analysis — Redesign

Переработанный вариант [оригинального проекта](https://github.com/Zdestr/consultant_plus_logs_search) с применением концепций всех 13 лекций курса «Построение пайплайнов данных».

---

## Архитектура

```
Log files
    │
    ▼  (Лекция 12: Kafka producer — acks=all, idempotent)
[Kafka: raw-session-logs]
    │
    ▼  (Лекция 4: RDD binaryFiles + stateful parser)
[Delta raw layer]   ← partitioned by date  (Лекция 7)
    │
    ▼  (Лекция 6: Predicate + Projection Pushdown)
[Delta processed layer]   ← MERGE INTO  (Лекция 7)
    │
    ▼  (Лекция 5: Broadcast Join; Лекция 6: Auto BHJ)
[Delta analytics layer]
    │
    ├──► Metric output (stdout / results.txt)
    └──► Prometheus Pushgateway  (Лекция 9/13)
```

Развёрнуто на Kubernetes: Spark Operator + Helm-чарт + Kustomize-оверлеи.
Оркестрация: Argo Workflows DAG.
CI/CD: GitHub Actions → registry → ArgoCD.

---

## Что изменилось относительно оригинала

| Аспект | Оригинал | Redesign |
|--------|----------|---------|
| Структура | 1 файл, 200 строк | 6 Scala-файлов, разделение по ответственности |
| Ingestion | `sc.wholeTextFiles` | `sc.binaryFiles` + Kafka-producer |
| Delta MERGE | `append` (дубли возможны) | `MERGE INTO` (идемпотентно) |
| Слои данных | 1 таблица sessions + doc_opens | raw / processed / analytics |
| Агрегации | `collect()` + scala sort | DataFrame groupBy + BroadcastHashJoin |
| Мониторинг | нет | Prometheus Pushgateway + alerting rules |
| Оркестрация | нет | Argo Workflows DAG |
| Деплой | `java -jar` | Spark Operator + Helm + Kustomize + CI/CD |
| ACID | базовый append | MERGE INTO + time travel |

---

## Применённые концепции по лекциям

### Лекция 2 — Data Product Canvas

Описан в [`data_product_canvas.md`](data_product_canvas.md).

### Лекция 3 — Форматы данных

Delta Lake хранит данные в Parquet (колоночный формат). Аналитические запросы читают только нужные колонки — в 5-10 раз меньше I/O по сравнению с CSV/JSON.

### Лекция 4 — RDD vs DataFrame

`IngestRaw.scala`: парсинг логов выполняется через `sc.binaryFiles()` + `flatMap(SessionParser.parse)`. Формат лога — нестандартный текстовый протокол с состоянием (SESSION_START → QS → результаты → DOC_OPEN). Нельзя выразить как SQL. После парсинга — немедленный переход на DataFrame для всех агрегаций.

### Лекция 5 — Оптимизации Spark

- `parsedRDD.cache()` — избегаем повторного парсинга при записи в два Delta-стола.
- `sc.longAccumulator("malformed_lines")` — thread-safe счётчик ошибок без `collect()`.
- `broadcast(metaDf)` — `doc_metadata` (5 строк) рассылается на все Executor-ы; Catalyst выбирает BroadcastHashJoin автоматически.

### Лекция 6 — Catalyst/Tungsten

В `ProcessSessions.scala` и `AggregateMetrics.scala`:
- **Projection Pushdown**: `.select("date", "doc_id")` → Parquet читает 2 из 4 колонок.
- **Predicate Pushdown**: `.filter(col("date") === targetDate)` → Delta передаёт предикат в Parquet-ридер, пропуская все партиции других дат.
- **Auto BroadcastHashJoin**: `metaDf` < 10 МБ → Catalyst автоматически broadcast-ит без явного `broadcast()`.

### Лекция 7 — Delta Lake

- **Трёхслойная архитектура**: `raw/` → `processed/` → `analytics/`
- **MERGE INTO (idempotent upsert)**: повторный запуск джобы не дублирует данные.
- **partitionBy("date")**: Predicate Pushdown в downstream-джобах (обрабатываем только один день).
- **Time travel**: `spark.read.format("delta").option("versionAsOf", 0).load(path)` — откат к предыдущей версии для дебага.
- **Schema evolution**: `mergeSchema=true` — добавление колонок без пересоздания таблицы.

### Лекция 8 — Kubernetes Resources

`k8s/namespace.yaml`: LimitRange устанавливает дефолты и MAX для всех контейнеров namespace.

`k8s/helm/consultant-plus/values.yaml` и `templates/spark-application.yaml`:

```yaml
driver:   { requests: { cpu: 1000m, memory: 2Gi }, limits: { cpu: 2000m, memory: 2Gi } }
executor: { requests: { cpu: 2000m, memory: 4Gi }, limits: { cpu: 4000m, memory: 4Gi } }
```

Memory requests = limits (жёсткий лимит предотвращает неожиданный OOM).

### Лекция 9/13 — Prometheus + Grafana

`PushMetrics.scala` использует prometheus simpleclient + PushGateway для публикации метрик после завершения Spark-джобы (pull-модель не успела бы сделать scrape до смерти pod-а).

Метрики: `cp_pipeline_files_total`, `cp_pipeline_card_search_hits`, `cp_pipeline_doc_opens_total`, `cp_pipeline_malformed_lines`.

`monitoring/alerts.yaml`: 4 alerting-правила (IngestJobNotRunning, HighMalformedLineRate, DeltaTableWriteErrors, KafkaConsumerLagHigh).

### Лекция 10 — Helm + Kustomize + CI/CD + GitOps

- **Helm-чарт** (`k8s/helm/consultant-plus/`): шаблонизированный SparkApplication + values для dev/prod.
- **Kustomize-оверлеи** (`k8s/overlays/`): dev (1 executor, dev-latest тег), prod (4 executors, фиксированный тег).
- **CI/CD** (`.github/workflows/ci.yaml`): test → build fat JAR → Docker push → обновить kustomize overlay → ArgoCD подхватит.

### Лекция 11 — Argo Workflows

`workflows/templates/spark-job-runner.yaml` — переиспользуемый WorkflowTemplate: принимает `command`, `image`, `logs-path`, `delta-path` как параметры. Использует Spark Operator через `resource` action.

`workflows/daily-session-pipeline.yaml` — основной DAG:

```
ingest → validate-raw → process → aggregate → push-metrics
```

### Лекция 12 — Kafka

`LogProducer.scala`: читает файлы логов, отправляет в топик `raw-session-logs`.
- `acks=all` + `retries=3` + `enable.idempotence=true` → at-least-once с дедупликацией на брокере.
- Ключ = `session_id` → сообщения одной сессии идут в одну партицию (порядок гарантирован).

---

## Запуск

```bash
mvn package -DskipTests

spark-submit \
  --class com.consultantplus.Main \
  --master local[*] \
  target/cp-pipeline-1.0.0.jar \
  all /path/to/logs /path/to/delta
```

Или через Argo:
```bash
argo submit workflows/daily-session-pipeline.yaml \
  -p date=2020-01-15 \
  -p logs-path=s3://cp-logs/sessions/ \
  -p delta-path=s3://cp-delta/
```

---

## Структура проекта

```
src/main/scala/com/consultantplus/
  SessionParser.scala          — парсер строк лога (RDD-совместимый)
  Main.scala                   — точка входа, роутинг команд
  jobs/
    IngestRaw.scala            — RDD → Delta raw (L4, L5, L7)
    ProcessSessions.scala      — Delta raw → processed (L6, L7)
    AggregateMetrics.scala     — processed → analytics (L5, L6, L7)
    PushMetrics.scala          — Prometheus Pushgateway (L9/13)
  kafka/
    LogProducer.scala          — Kafka producer (L12)
k8s/
  namespace.yaml               — LimitRange + ResourceQuota (L8)
  helm/consultant-plus/        — Helm-чарт (L10)
  overlays/{dev,prod}/         — Kustomize (L10)
workflows/
  templates/                   — WorkflowTemplates (L11)
  daily-session-pipeline.yaml  — DAG-пайплайн (L11)
monitoring/
  alerts.yaml                  — Prometheus rules (L9/13)
  prometheus.yml               — scrape config (L9/13)
.github/workflows/ci.yaml      — CI/CD (L10)
data_product_canvas.md         — Data Product Canvas (L2)
```
