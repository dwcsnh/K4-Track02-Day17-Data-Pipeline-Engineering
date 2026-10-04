# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:**
**Repo:**
**Commit bài nộp:**
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):**
**Nguồn tham khảo khác (nếu có):**

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `test_silver_tickets_one_row_per_ticket` và `test_silver_tickets_latest_state_wins` fail; `silver_tickets` có 24 dòng thay vì 12; T-91 có 3 hàng với trạng thái cũ/mới cùng xuất hiện thay vì trạng thái cuối `high / closed / bug`. | `test_feature_daily_reconciles_with_full_recompute`, `test_late_events_land_in_their_event_day`, `test_lookback_covers_measured_lateness` fail; `gold_feature_daily` lệch checksum với full recompute; u05 ngày 2026-08-12 chỉ ghi nhận `(2, 1, 0)` thay vì `(5, 3, 1)`. | `test_cdc_delete_becomes_tombstone`, `test_deleted_ticket_leaves_training_and_rag` fail; verify báo T-97 còn `is_deleted = False` và còn nguyên thông tin cá nhân ở Silver; T-97 vẫn còn trong training snapshot mới nhất (1 hàng) và RAG chunks (1 chunk). |
| **Nguyên nhân gốc** | `upsert_silver_tickets` chỉ dedup CDC changes trong nội bộ một batch bằng `row_number()`, rồi dùng `INSERT INTO` vào `silver_tickets`. Qua nhiều batch, các bản ghi mới bị chèn thêm thay vì upsert theo khoá `ticket_id`, và không dùng thứ tự LSN để bảo vệ trạng thái khi replay batch cũ. | `LOOKBACK_DAYS = 0`, daily run chỉ recompute duy nhất ngày hiện tại `[day, day]`. Khi các event xảy ra ngày 12/08 nhưng bị giao trễ vào ngày 15/08, partition ngày 12/08 không được tính lại nên bỏ lọt các event đến muộn. | `ticket_changes_sql` chỉ trích xuất `ticket_id` từ `after`. Khi có CDC delete (`_op = 'd'`), Debezium gửi `after = null`, khiến `ticket_id` bị null và bị loại bởi `WHERE ticket_id IS NOT NULL`. Thao tác xoá không bao giờ tới được Silver. |
| **Cách sửa** (file, vài dòng) | Sửa [pipeline/silver.py](../pipeline/silver.py): thay `INSERT INTO` bằng `MERGE INTO silver_tickets AS t USING _latest_changes AS s ON t.ticket_id = s.ticket_id WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE SET ... WHEN NOT MATCHED THEN INSERT VALUES (...)`. | Sửa [pipeline/config.py](../pipeline/config.py): đổi `LOOKBACK_DAYS = 0` thành `LOOKBACK_DAYS = 3` (dựa trên P99 đo được = 3.00 ngày) để cơ chế overwrite-partition tính lại cửa sổ `[day - 3, day]`. | Sửa [pipeline/staging.py](../pipeline/staging.py): dùng `coalesce(j->'value'->'after'->>'ticket_id', j->'value'->'before'->>'ticket_id', j->'key'->>'ticket_id') AS ticket_id` để lấy khoá ticket từ `before` hoặc `key` khi `after` là `null`. |
| **Khái niệm trên slide** | Keyed MERGE (Upsert theo khoá tự nhiên `ticket_id`) kết hợp LSN Guard (kiểm tra thứ tự Log Sequence Number để đảm bảo Idempotency khi replay batch cũ). | Event Time vs. Ingest Time, Watermark/Lookback window và Overwrite-partition (recompute các partition trong cửa sổ trễ để hấp thụ late data). | CDC Delete vs. Kafka Tombstone, Tombstone pattern ở Silver (`is_deleted = true`, mask PII) và LSN Guard chống hồi sinh ticket khi replay batch cũ. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: `silver_tickets` biểu diễn thực thể có khoá tự nhiên (`ticket_id`) cập nhật từng dòng theo thứ tự CDC (`_lsn`), còn `gold_feature_daily` là bảng số đo tổng hợp theo ngày nên ghi đè toàn bộ phân vùng (partition) của ngày đó khi có dữ liệu đến muộn.
- Tombstone thay vì xoá hẳn hàng trong Silver: Lưu tombstone kèm `_lsn` cao hơn giúp ngăn chặn việc replay các batch cũ vô tình "hồi sinh" (resurrect) bản ghi đã xoá, đồng thời xoá sạch dữ liệu PII để tuân thủ quyền được xoá dữ liệu.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: Đảm bảo tính bất biến và tái lập (point-in-time reproducibility) cho các phiên bản mô hình ML trong quá khứ, trong khi các snapshot mới tự động loại bỏ bản ghi đã xoá.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: Dữ liệu ở quy mô vừa và nhỏ (vài chục MB đến vài chục GB) xử lý hiệu quả nhất với DuckDB/dbt in-process (tốc độ sub-second, zero-cost, không cần cụm máy chủ), trong khi Spark có chi phí vận hành cao và overhead lớn cho việc khởi tạo cluster, phân mảnh mạng và serialization.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày
   08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ
   đặt chốt PII nào, ở tầng nào, và đo nó ra sao?

## 5. Output (dán nguyên văn)

```text
$ make verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ make test
..................................                                       [100%]
34 passed in 2.50s

$ make rerun3
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ make lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ make dbt
cd dbt_project && DBT_PROFILES_DIR=. "/home/ducanh/Documents/AI20K/Lab/Phase 2/Day 2/K4-Track02-Day17-Data-Pipeline-Engineering/.venv/bin/dbt" build --event-time-start 2026-08-10 --event-time-end 2026-08-17
Running with dbt=1.12.5
Registered adapter: duckdb=1.11.0
Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test

Concurrency: 1 threads (target='dev')

1 of 19 START sql view model main.stg_events ................................... [RUN]
1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.08s]
2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.03s]
3 of 19 START sql incremental model main.silver_events ......................... [RUN]
3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.22s]
4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.14s]
8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.10s]
5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.04s]
6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.02s]
7 of 19 START test unique_silver_events_event_id ............................... [RUN]
7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.03s]
9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.02s]
10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.02s]
11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.02s]
12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.02s]
13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.02s]
14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.02s]
15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.03s]
16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.03s]
Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.06s]
Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.03s]
Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.05s]
Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.06s]
Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.05s]
Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.04s]
16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.36s]
17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.02s]
18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.02s]
19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.02s]

Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 1.41 seconds (1.41s).

Completed successfully

Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

Nếu dùng PowerShell, ghi lệnh tương đương và output thực tế theo [SUBMISSION.md](../docs/SUBMISSION.md).
Nếu làm bonus, thêm output B1 hoặc đường dẫn bằng chứng B2 ở cuối phần này.
