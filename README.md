# ConsultantPlus Log Analysis

Spark-задача на Scala для анализа пользовательских сессий системы КонсультантПлюс.

## Задача

По 10 000 файлам сессий вычислить:

1. **Metric 1** — сколько раз документ `ACC_45616` встречался в результатах поиска через карточку поиска (`CARD_SEARCH`)
2. **Metric 2** — количество открытий каждого документа, найденного через быстрый поиск (`QS`), за каждый день

## Формат данных

Каждый файл — одна сессия. Возможные события:

```
SESSION_START 01.07.2020_13:40:50
QS 01.07.2020_13:42:01 {текст запроса}
<search_id> <doc1> <doc2> ...
CARD_SEARCH_START 01.07.2020_13:45:00
$<param_id> <значение>
CARD_SEARCH_END
<search_id> <doc1> <doc2> ...
DOC_OPEN 01.07.2020_13:46:00 <search_id> <doc_id>
SESSION_END 01.07.2020_13:50:00
```

## Стек

- Scala 2.12
- Apache Spark 3.5.3 (local mode)
- Delta Lake 3.2.1 (LakeHouse-кеш результатов)
- Apache Maven

## Сборка

```bash
mvn package
```

Создаёт fat-jar `target/log-analysis-1.0-SNAPSHOT.jar` со всеми зависимостями.

## Запуск

```bash
java \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  -jar target/log-analysis-1.0-SNAPSHOT.jar \
  /path/to/docs \
  /path/to/delta
```

- `/path/to/docs` — директория с файлами сессий
- `/path/to/delta` — директория для Delta Lake-таблиц (создаётся автоматически при первом запуске)

При повторном запуске уже обработанные файлы пропускаются; метрики вычисляются по кешированным данным в Delta-таблицах.

> `--add-opens` требуется для Java 17+ из-за модульной системы

## Результаты

### Metric 1

```
Card searches that returned ACC_45616
Count: 479
```

### Metric 2

Полная таблица в файле [`results.txt`](results.txt). Формат:

```
Date            Document ID          Opens
---------------------------------------------
01.01.2020      ACC_45614            3
01.01.2020      ACC_45615            7
01.01.2020      ACC_45616            1
...
26.12.2020      RLAW411_173686       1
```

Всего: **75 516** уникальных пар (дата, документ) за период 01.01.2020 – 26.12.2020.
