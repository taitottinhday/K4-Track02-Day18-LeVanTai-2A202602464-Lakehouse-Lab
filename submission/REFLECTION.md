# Reflection — external index không phải source of truth

Một anti-pattern nguy hiểm với RAG là coi vector index ngoài lakehouse là nguồn dữ
liệu chính. Đồng bộ ban đêm có thể nhanh, nhưng khi tài liệu bị sửa, hết hạn hoặc có
yêu cầu xoá, embedding cũ vẫn bị truy hồi. Với kho tài liệu nội bộ, lỗi này vừa trả lời
sai phiên bản vừa có thể lộ nội dung đã thu hồi.

Tôi sẽ để Delta/Iceberg chứa `document_id`, version, quyền truy cập và embedding làm
system of record; ANN index chỉ là bản dẫn xuất có thể rebuild. Mỗi thay đổi phát CDF
gồm cả delete; consumer lưu watermark và kiểm tra lag. Trước khi publish index, job
đối chiếu ID, version và mẫu truy vấn với bảng hiện hành. Với xoá khẩn, gateway chặn
ID, consumer xử lý delete rồi audit eviction. Corpus/index version được ghi vào
retrieval log để tái hiện kết quả.

AI hỗ trợ đọc rubric và diễn giải khái niệm; tôi tự chạy, kiểm tra và giải thích output.
