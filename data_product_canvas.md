# Data Product Canvas — ConsultantPlus Session Analytics

**Лекция 2 — Аналитика для инженеров**

---

## User Stories

> *Как аналитик поисковой команды, я хочу знать, как часто документ ACC_45616 появляется в результатах карточного поиска, чтобы принять решение о его позиции в ранжировании.*

> *Как product manager, я хочу видеть ежедневную динамику открытий документов, найденных через быстрый поиск, чтобы понять, какие документы наиболее востребованы после поиска.*

> *Как инженер данных, я хочу получать алерт если пайплайн не запускался более 24 часов, чтобы устранить сбой до того, как данные критически устареют.*

---

## Vision & Goals

**Vision:** Надёжный инкрементальный пайплайн, который обрабатывает сессионные логи 10 000+ файлов и даёт аналитикам актуальные данные о поведении пользователей в поиске, без ручного перезапуска.

**Goals:**

| Цель | Метрика успеха |
|------|---------------|
| Свежесть данных | Метрики доступны ≤ 4 часов после появления новых логов |
| Надёжность | Повторный запуск не дублирует и не теряет данные (MERGE INTO) |
| Наблюдаемость | Prometheus alert срабатывает при просе > 24ч без обработки |

---

## Constraints

| Тип | Ограничение |
|-----|------------|
| **Данные** | Логи — нестандартный текстовый формат; традиционный Spark CSV/JSON reader неприменим |
| **Инфраструктура** | Spark запускается в local[*] режиме при разработке; кластерный режим через Spark Operator |
| **Идемпотентность** | Один и тот же файл не должен обрабатываться дважды (решение: MERGE INTO по file_path) |

---

## Users

- **Аналитик поиска** — потребляет `analytics/summary`: открытия документов по дням
- **Дежурный инженер** — смотрит Grafana-дашборд, получает алерты
- **Data engineer** — запускает и мониторит пайплайн

---

## Inputs & Sources

| Источник | Формат | Объём |
|---------|--------|-------|
| Session log files | Текстовые .log (нестандартный протокол: SESSION_START, QS, CARD_SEARCH_*, DOC_OPEN) | ~10 000 файлов / год |
| Kafka topic `raw-session-logs` | Binary (bytes) | опционально, для streaming |

---

## Outputs & Schema

### `raw/sessions` (Delta, partitioned by date)
```
file_path        string   — абсолютный путь к файлу лога
date             string   — дата сессии (из SESSION_START)
card_search_hits int      — количество hits на ACC_45616 в карточном поиске
malformed_lines  int      — количество пропущенных строк
```

### `processed/daily_opens` (Delta, partitioned by date)
```
date    string  — дата открытия документа
doc_id  string  — идентификатор документа (ACC_45616, LAW_123, …)
opens   long    — количество открытий через быстрый поиск
```

### `analytics/summary` (Delta, partitioned by date)
```
date      string  — дата
doc_id    string  — идентификатор документа
opens     long    — открытий через быстрый поиск
category  string  — тип документа (Законодательство, Арбитражная практика, …)
```

---

## Quality & SLA

| Метрика | Проверка | Порог |
|---------|---------|-------|
| Freshness | `time() - cp_pipeline_last_ingest_timestamp` | ≤ 24ч, иначе critical alert |
| Malformed rate | `malformed_lines / total_lines` | < 5%, иначе warning alert |
| Completeness | row count в processed ≥ min_rows после каждого запуска | ≥ 1000 (delta-validator WorkflowTemplate) |

---

## Infrastructure & Pipeline

```
Raw logs (S3/HDFS)
    │
Kafka producer → [raw-session-logs]
    │
IngestRaw.scala   sc.binaryFiles + RDD parser → Delta raw
    │
ProcessSessions.scala   DataFrame groupBy MERGE INTO → Delta processed
    │
AggregateMetrics.scala  BroadcastHashJoin → Delta analytics
    │
PushMetrics.scala  → Prometheus Pushgateway
    │
Grafana дашборд + AlertManager
```

**Оркестрация:** Argo Workflows DAG (daily-session-pipeline.yaml)
**Деплой:** Spark Operator + Helm + Kustomize overlays (dev/prod)
**CI/CD:** GitHub Actions → Docker registry → ArgoCD
