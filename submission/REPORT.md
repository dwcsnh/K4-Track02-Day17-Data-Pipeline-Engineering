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
- `make parity`: PARITY / MISMATCH

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: `silver_tickets` biểu diễn thực thể có khoá tự nhiên (`ticket_id`) cập nhật từng dòng theo thứ tự CDC (`_lsn`), còn `gold_feature_daily` là bảng số đo tổng hợp theo ngày nên ghi đè toàn bộ phân vùng (partition) của ngày đó khi có dữ liệu đến muộn.
- Tombstone thay vì xoá hẳn hàng trong Silver: Lưu tombstone kèm `_lsn` cao hơn giúp ngăn chặn việc replay các batch cũ vô tình "hồi sinh" (resurrect) bản ghi đã xoá, đồng thời xoá sạch dữ liệu PII để tuân thủ quyền được xoá dữ liệu.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: Đảm bảo tính bất biến và tái lập (point-in-time reproducibility) cho các phiên bản mô hình ML trong quá khứ, trong khi các snapshot mới tự động loại bỏ bản ghi đã xoá.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark:

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày
   08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ
   đặt chốt PII nào, ở tầng nào, và đo nó ra sao?

## 5. Output (dán nguyên văn)

```text
$ make verify

$ make test

$ make rerun3

$ make lateness

$ make dbt

$ make parity
```

Nếu dùng PowerShell, ghi lệnh tương đương và output thực tế theo [SUBMISSION.md](../docs/SUBMISSION.md).
Nếu làm bonus, thêm output B1 hoặc đường dẫn bằng chứng B2 ở cuối phần này.
