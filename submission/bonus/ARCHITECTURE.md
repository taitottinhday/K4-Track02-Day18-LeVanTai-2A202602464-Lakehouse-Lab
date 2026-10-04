# Bonus — Corpus huấn luyện 1 nghìn tỷ token có provenance và khả năng thu hồi

**Người viết:** Lê Văn Tài — **MSSV:** 2A202602464
**Bài toán chọn:** B — Trillion-token training corpus với provenance
**Phạm vi:** thiết kế giả định để design review, không phải tuyên bố tuân thủ pháp lý.

## 1. Problem statement

Nền tảng thu thập **30 TB** web crawl ở US, khử trùng lặp xuống **3 TB** và xuất bản
**1 TB** corpus huấn luyện ở EU. Mỗi shard phải mang license/provenance; `unknown` không
được đi vào tập train. Khi một shard bị báo contamination, đội vận hành phải trả lời
checkpoint/model nào đã dùng nó, tạo corpus đã loại shard trong **≤30 phút**, và bắt đầu
re-train downstream. US là nơi ingest; dữ liệu curated được phép chuyển sang EU theo
chính sách tổ chức, raw crawl không được mặc định sao chép sang EU. Mục tiêu không phải
"lưu nhiều file", mà là một quyết định có thể audit: *tại thời điểm train, exactly which
snapshot và shard-set đã được dùng?*

Giả định planning: 30 TB là một đợt ingest, 3 TB Silver và 1 TB Gold là trạng thái hoạt
động; shard Parquet mục tiêu 512 MB; 5% dung lượng active dành cho metadata, delete files
và snapshot headroom. Các giá dưới là **assumptions để lập ngân sách**, cần xác nhận lại
bằng Pricing Calculator theo region/contract trước khi mua.

## 2. Kiến trúc được đề xuất

```text
 US region                                                        EU region
 ┌──────────────┐     quarantine + PII scan     ┌──────────────────────────┐
 │ crawler/CDC  │ ───────────────────────────►  │ Bronze Iceberg (30 TB)   │
 │ URL, bytes,  │     immutable raw + hash      │ raw, restricted, 30-day  │
 │ source facts │                               └──────────┬───────────────┘
 └──────────────┘                                          │ MERGE + dedup
                                                           ▼
                                              ┌──────────────────────────┐
                                              │ Silver Iceberg (3 TB)    │
                                              │ MinHash/LSH, license,    │
                                              │ provenance, quality      │
                                              └──────────┬───────────────┘
                                              approved encrypted export │
                                                           ▼
 ┌────────────────────────────────────────────────────────────────────────┐
 │ Nessie/REST catalog: schemas, partition specs, snapshots, branches      │
 └───────────────────────────────────────┬────────────────────────────────┘
                                         EU │
                                            ▼
                              ┌───────────────────────────────┐
                              │ Gold curated corpus (1 TB)    │
                              │ license allow-list; 512MB      │
                              │ shards; partition: language / │
                              │ source_family / ingest_month  │
                              └───────────┬───────────────────┘
                              snapshot ID │ pinned in run card
                                            ▼
                   ┌────────────────────────────────────────────┐
                   │ trainer + model/checkpoint lineage registry │
                   │ run_id → catalog ref/snapshot → shard IDs   │
                   └────────────────────────────────────────────┘

 Maintenance: compact small files • expire snapshots by retention • orphan sweep
 Incident: branch from last approved snapshot → exclusion MERGE → validate → fast-forward
```

Sáu khái niệm Day18 được gắn vào lựa chọn cụ thể: Bronze→Silver→Gold tách raw khỏi
curated; Iceberg catalog là control plane; hidden partitioning tránh bắt analyst biết cột
phái sinh; snapshot/time travel và Nessie branch là rollback; compaction/expiry/orphan
sweep là chi phí vận hành; provenance là cột và partition/metadata bắt buộc, không phải
tag ở wiki.

## 3. Năm quyết định chính và các lựa chọn bị loại

### D1 — Iceberg + REST catalog + Nessie branch cho corpus train

**Chọn:** Iceberg cho Silver/Gold; REST catalog để Spark/Trino/DuckDB nhìn cùng metadata;
Nessie quản lý reference/branch phát hành corpus.

- **Loại Delta-only:** Delta có transaction log và time travel tốt, nhưng thiết kế này cần
  publish branch/tag corpus qua nhiều engine và catalog-neutral control plane; chọn Iceberg
  giảm coupling của reader. Đây không phải nhận định Delta “kém”; Delta hợp lý nếu workload
  đã chuẩn hoá Databricks và không cần branch workflow.
- **Loại Hive files + partitions thủ công:** không có atomic snapshot, rename/partition
  evolution dễ biến thành rewrite, và một reader quên partition predicate sẽ full scan.
- **Loại copy toàn bộ 1 TB cho mỗi release:** release nhanh ban đầu nhưng nhân đôi storage,
  không cung cấp lineage ở mức shard và rollback chậm khi corpus phát hành nhiều lần.

### D2 — Medallion với raw immutable và policy gate ở Silver→Gold

**Chọn:** Bronze lưu raw bytes/URL/hash và access-restricted trong 30 ngày; Silver chuẩn hoá,
dedup và gắn `source_id`, `license`, `license_evidence_uri`, `consent_state`,
`ingest_run_id`; Gold chỉ materialize bản ghi đạt allow-list. `UNKNOWN` và thiếu evidence
vào quarantine, không có default-to-allowed.

- **Loại lọc ngay tại crawler:** parser/license classifier thay đổi; bỏ raw làm mất khả năng
  tái phân loại và điều tra false positive.
- **Loại "một bảng sạch" chung:** truy vấn train có thể vô tình đọc record chưa duyệt;
  schema và retention khác nhau khiến quyền raw lan ra diện rộng.
- **Loại provenance JSON không schema:** không thể enforce nullability/allow-list, không
  prune theo nguồn và khó join checkpoint→shard khi có incident.

### D3 — 512 MB Parquet, hidden partitions theo `language`, `source_family`, `month(ingested_at)`

**Chọn:** Iceberg transform partitions; job compaction đưa data files về gần 512 MB và sort
trong file theo `(language, source_family, content_hash)`. Tokenizer/trainer lọc trên các
cột nguồn, catalog tự suy partition.

- **Loại partition `license` cấp cao:** cardinality và skew gây nhiều file nhỏ; license vẫn
  là stats/filter column và security policy, không phải layout primary.
- **Loại partition mỗi ngày/URL domain:** 30 TB crawl sinh quá nhiều partitions/manifests;
  planning metadata tăng và compaction tốn tiền.
- **Loại unpartitioned 1-TB table:** cập nhật/rebuild theo language hoặc source-family quét
  quá nhiều files, kéo dài vượt SLA 30 phút.

### D4 — Reproducibility bằng immutable snapshot ID, run card và model-lineage registry

**Chọn:** mỗi training run ghi `catalog_uri`, table identifier, Nessie ref *và commit hash*,
Iceberg `snapshot_id`, danh sách shard manifest checksum, tokenizer/container digest và
output checkpoint URI. Promotion chỉ chấp nhận run card đầy đủ.

- **Loại ghi tên dataset kiểu `gold-v7`:** tên có thể bị overwrite; không thể trả lời bản
  v7 nào khi bản mới phát hành.
- **Loại dựa vào timestamp:** clock/late commits không xác định snapshot; query as-of time
  dễ khác giữa engine.
- **Loại lưu chỉ checkpoint metadata:** biết model nhưng không biết shard đã train; không
  truy vết contamination đến batch/checkpoint.

### D5 — Thu hồi bằng branch + exclusion manifest, không xoá nóng history

**Chọn:** incident service tìm `source_id/content_hash` trong manifest, tạo branch từ
last-approved snapshot, chạy equality delete/MERGE exclude, validate count/license và
fast-forward `release/approved`. Training mới nhận snapshot mới; run cũ vẫn giữ evidence.
Raw retention và snapshot expiry theo policy chỉ chạy sau legal/reader safety window.

- **Loại `DELETE` trực tiếp ở main:** làm hỏng reproducibility của run đang đọc và khó
review thay đổi trước publish.
- **Loại vacuum/expire ngay lập tức:** xóa physical file trước khi checkpoint/audit truy
vết xong; reader đang chạy có thể lỗi.
- **Loại chỉ chặn shard trong trainer:** source-of-truth còn bẩn, mọi engine khác vẫn có thể
đọc shard và không có release snapshot sạch cho lần sau.

### D6 — Mã hoá, least privilege và vùng dữ liệu US/EU tách rời

**Chọn:** bucket/KMS key theo region và layer, service role per job, raw US không được grant
cho EU trainer; approved Gold export qua allow-list + audit log. Catalog trả credentials có
thời hạn ngắn, không phát URI/bucket key rộng rãi.

- **Loại một bucket/key toàn cầu:** đơn giản nhưng blast radius và khó chứng minh vị trí dữ
  liệu/quyền truy cập.
- **Loại replicate 30 TB raw sang EU:** tăng chi phí, delay và rủi ro policy; EU cần 1 TB
  Gold đã duyệt chứ không cần raw.
- **Loại client-side CSV export:** mất snapshot semantics, encryption/audit không nhất quán
  và tạo shadow copies không có lifecycle.

## 4. Vận hành lúc 3 giờ sáng: detect và rollback

| Failure mode | Phát hiện | Cô lập/rollback |
|---|---|---|
| License classifier bug gán `unknown` thành allowed | metric `unknown→allowed` tăng đột biến; policy test trên hold-out fixtures fail | đóng promotion, branch từ approved snapshot trước đó, re-run classifier trên Silver, publish Gold snapshot mới; không xoá history trước audit |
| Dedup/compaction tạo small files hoặc duplicate bùng lên | manifest/file count, median file size và unique `content_hash` SLO; query planning latency alert | dừng writer, revert catalog ref về snapshot tốt, compact staging branch; chỉ fast-forward sau khi count/hash reconciliation pass |
| Contamination report cho một shard đã train | registry trả `snapshot_id → manifest → shard`; incident timer bắt đầu | block release ref, tạo exclusion branch và thực hiện equality delete, test corpus; checkpoint nào chứa shard được đánh dấu re-train; không dùng `vacuum` như rollback |
| EU export có record raw hoặc thiếu approval | DLP/export schema contract + destination bucket policy denied-event alert | revoke temporary credentials, quarantine export object, restore published ref nếu đã promote; điều tra audit log |
| Snapshot expiry/orphan sweep quá sớm | job dry-run liệt kê candidates; active-reader lease và minimum retention gate | cancel delete phase, retain manifest/files, restore catalog reference/snapshot nếu metadata còn; change retention only after drill |

Hai failure đầu gắn trực tiếp với Day18: snapshot branch bảo toàn time travel khi policy/dedup sai;
maintenance không được nhầm “unreferenced” với “đã xóa vật lý”. Runbook bắt buộc dry-run,
approval và evidence link trước destructive phase.

## 5. Ước lượng chi phí hàng tháng

**Giả định rate planning (US/EU phải re-price trước ký hợp đồng):** S3 Standard $23/TB-tháng,
S3 Glacier Deep Archive $0.99/TB-tháng, cross-region transfer $20/TB, Spark-like compute
$0.44/DPU-hour. S3 công bố rằng lifecycle có thể chuyển/expire objects và Deep Archive có
minimum 180 ngày; vì vậy archival không phải cách giảm chi phí cho raw chỉ giữ 30 ngày.
[S3 pricing](https://aws.amazon.com/s3/pricing/) và
[S3 lifecycle guidance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html).

| Thành phần | Phép tính | USD/tháng |
|---|---:|---:|
| US Bronze hot 30 ngày | 30 TB × $23 | $690.00 |
| US Silver active | 3 TB × $23 | $69.00 |
| EU Gold active | 1 TB × $23 | $23.00 |
| Snapshot/delete/metadata headroom | (3 + 1) TB × 5% × $23 | $4.60 |
| Long-term model/run evidence archive | 2 TB × $0.99 | $1.98 |
| Approved US→EU Gold transfer | 1 TB × $20 | $20.00 |
| Dedup + curate + export compute | 1,200 DPU-h × $0.44 | $528.00 |
| Monthly incident/release reserve | 300 DPU-h × $0.44 | $132.00 |
| **Tổng planning** |  | **$1,468.58/tháng** |

Số này cố ý không giấu two cost drivers: reprocess compute (45%) và Bronze hot retention
(47%). Nếu crawl trở thành 30 TB **mỗi ngày**, kiến trúc/storage budget trên không còn hợp lệ:
Bronze steady state thành ~900 TB × $23 = $20,700/tháng trước compute. Khi đó phải thay
đổi constraint (retention, sampling, compression) chứ không dùng lifecycle để “làm biến
mất” chi phí. Intelligent-Tiering hợp với access pattern không đoán được nhưng có monitoring
fee và không thay thế policy retention; xem [AWS guidance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/intelligent-tiering.html).

## 6. MVP một tuần: chứng minh đường khó nhất

**Mục tiêu slice:** 10 GB synthetic crawl, 20 shards, hai license hợp lệ và một `unknown`.
Một pipeline tạo Bronze→Silver (dedup+provenance)→Gold allow-list, rồi tạo một run card
đã pin snapshot. Một incident giả định blacklist 1 shard phải tạo release branch sạch.

**Kế hoạch:**

1. Ngày 1: local REST/SQL catalog, Iceberg tables và schema contract required fields.
2. Ngày 2: ingest Bronze immutable + deterministic `content_hash`; test duplicate và schema
   enforcement.
3. Ngày 3: Silver classification/dedup; Gold allow-list; query `unknown=0` như release gate.
4. Ngày 4: record snapshot/run-card/manifest checksum; replay reader đúng snapshot.
5. Ngày 5: simulate contamination, branch/exclude/validate/publish; đo elapsed time.

**Acceptance criteria:** (a) Gold chứa 0 `unknown`; (b) replay `snapshot_id` cho đúng số
row và cùng shard checksums; (c) branch exclusion loại đúng shard mà không thay đổi main
trước approval; (d) end-to-end incident branch-to-approved-ref hoàn tất ≤30 phút trên 10 GB;
(e) compaction giữ median file 256–512 MB and `plan_files()` pruning được chứng minh khi
lọc theo source/language. Mechanism khó nhất là *stable, auditable association từ checkpoint
đến immutable corpus snapshot*, nên được test trước UI, dashboard hay ANN index.

## 7. Kết luận review

Đề xuất đánh đổi một ít metadata/operational discipline để đổi lấy một vật thể có thể kiểm
toán: catalog reference + snapshot + manifest checksum. Nếu không có ba khóa đó, "remove
shard trong 30 phút" chỉ là thay đổi file chứ không chứng minh được model nào bị ảnh hưởng.
Production gate là evidence có thể truy vết, không phải một query trả PASS.
