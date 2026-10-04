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
| **Triệu chứng** | `test_silver_tickets_one_row_per_ticket` và `test_silver_tickets_latest_state_wins` fail; `silver_tickets` có 24 dòng thay vì 12; T-91 có 3 hàng với trạng thái cũ/mới cùng xuất hiện thay vì trạng thái cuối `high / closed / bug`. | | |
| **Nguyên nhân gốc** | `upsert_silver_tickets` chỉ dedup CDC changes trong nội bộ một batch bằng `row_number()`, rồi dùng `INSERT INTO` vào `silver_tickets`. Qua nhiều batch, các bản ghi mới bị chèn thêm thay vì upsert theo khoá `ticket_id`, và không dùng thứ tự LSN để bảo vệ trạng thái khi replay batch cũ. | | |
| **Cách sửa** (file, vài dòng) | Sửa [pipeline/silver.py](file:///home/ducanh/Documents/AI20K/Lab/Phase%202/Day%202/K4-Track02-Day17-Data-Pipeline-Engineering/pipeline/silver.py): thay `INSERT INTO` bằng `MERGE INTO silver_tickets AS t USING _latest_changes AS s ON t.ticket_id = s.ticket_id WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE SET ... WHEN NOT MATCHED THEN INSERT VALUES (...)`. | | |
| **Khái niệm trên slide** | Keyed MERGE (Upsert theo khoá tự nhiên `ticket_id`) kết hợp LSN Guard (kiểm tra thứ tự Log Sequence Number để đảm bảo Idempotency khi replay batch cũ). | | |

## 2. Các con số

- P99 lateness đo từ Bronze: `____` ngày → `LOOKBACK_DAYS = ____`
- `submission/checksums.txt`: PASS / FAIL — Gold checksum: `________________`
- `make parity`: PARITY / MISMATCH

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: `silver_tickets` biểu diễn thực thể có khoá tự nhiên (`ticket_id`) cập nhật từng dòng theo thứ tự CDC (`_lsn`), còn `gold_feature_daily` là bảng số đo tổng hợp theo ngày nên ghi đè toàn bộ phân vùng (partition) của ngày đó khi có dữ liệu đến muộn.
- Tombstone thay vì xoá hẳn hàng trong Silver:
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ:
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
