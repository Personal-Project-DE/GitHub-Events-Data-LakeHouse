# Metabase BI & Dashboard Configuration Guide

- **Target System:** Metabase v0.50.28 connected to Trino 468 (`iceberg.gold`)
- **Port:** `3000` (Default URL: `http://localhost:3000`)
- **Underlying Engine:** Trino Distributed SQL Engine

---

## 1. Kết Nối Trino Trong Metabase

Sau khi khởi chạy container `lakehouse-metabase`, truy cập vào `http://localhost:3000` để hoàn tất thiết lập tài khoản ban đầu và thêm nguồn dữ liệu Trino:

1. **Database Type:** `Starburst / Trino` (hoặc `Presto / Trino`)
2. **Display Name:** `Lakehouse Trino - Gold Marts`
3. **Host:** `trino` (kết nối nội bộ qua Docker network `lakehouse-net`) hoặc `localhost` (nếu chạy Metabase ngoài container)
4. **Port:** `8080` (cổng nội bộ container Trino) hoặc `8082` (cổng ánh xạ host)
5. **Catalog:** `iceberg`
6. **Schema:** `gold`
7. **Username:** `admin` (hoặc `lakehouse`)
8. **Password:** Để trống (Trino cấu hình không yêu cầu mật khẩu trên mạng local)
9. **SSL:** `Disabled`

---

## 2. Chi Tiết 4 Biểu Đồ Analytical Dashboard

Dashboard tổng thể: **"GitHub Events Lakehouse Analytics"**

### Card 1: Top Daily Trending Repositories
* **Loại trực quan:** Bảng xếp hạng (Table) kết hợp Biểu đồ cột (Bar Chart).
* **Mục đích:** Xác định các kho lưu trữ có mức độ tăng trưởng đột biến về sao (Stars), Forks, Pull Requests và số lượng cộng tác viên thực tế (con người).
* **Bảng dữ liệu:** `iceberg.gold.mart_trending_repos_daily`
* **Câu truy vấn SQL (Trino):**
```sql
SELECT 
    metric_date AS "Metric Date",
    repo_name AS "Repository",
    primary_language AS "Primary Language",
    new_stars_count AS "New Stars",
    forks_count AS "Forks",
    opened_prs_count AS "Opened PRs",
    merged_prs_count AS "Merged PRs",
    distinct_human_contributors AS "Human Contributors"
FROM iceberg.gold.mart_trending_repos_daily
WHERE metric_date = (SELECT MAX(metric_date) FROM iceberg.gold.mart_trending_repos_daily)
ORDER BY new_stars_count DESC, forks_count DESC
LIMIT 20
```

---

### Card 2: Human vs. Bot Developer Activity
* **Loại trực quan:** Biểu đồ tròn / Donut Chart hoặc Stacked Bar.
* **Mục đích:** Phân định tỷ lệ hoạt động mã nguồn thực tế đến từ lập trình viên (Human) so với các công cụ tự động hóa, CI/CD, dependency bots (Dependabot, Renovate, GitHub Actions, Snyk, etc.).
* **Bảng dữ liệu:** `iceberg.silver.fact_engagements` kết hợp `iceberg.silver.dim_users`
* **Câu truy vấn SQL (Trino):**
```sql
SELECT 
    CASE 
        WHEN u.is_bot THEN 'Automated Bots (CI/CD, Renovate, Dependabot)'
        ELSE 'Human Developers'
    END AS "Actor Type",
    COUNT(f.event_id) AS "Total Events",
    COUNT(DISTINCT f.actor_id) AS "Unique Actors"
FROM iceberg.silver.fact_engagements f
JOIN iceberg.silver.dim_users u ON f.actor_id = u.user_id
GROUP BY u.is_bot
ORDER BY "Total Events" DESC
```

---

### Card 3: UTC Developer Work Patterns Heatmap
* **Loại trực quan:** Heatmap / Matrix 2D (Ngày trong tuần $\times$ Giờ trong ngày theo chuẩn giờ quốc tế UTC).
* **Mục đích:** Phân tích nhịp sinh học và khung giờ làm việc tích cực nhất của cộng đồng lập trình viên trên toàn cầu (chỉ tính đóng góp từ con người `is_bot = FALSE`).
* **Bảng dữ liệu:** `iceberg.gold.mart_developer_work_patterns_daily`
* **Câu truy vấn SQL (Trino):**
```sql
SELECT 
    CASE day_of_week
        WHEN 1 THEN '1 - Monday'
        WHEN 2 THEN '2 - Tuesday'
        WHEN 3 THEN '3 - Wednesday'
        WHEN 4 THEN '4 - Thursday'
        WHEN 5 THEN '5 - Friday'
        WHEN 6 THEN '6 - Saturday'
        WHEN 7 THEN '7 - Sunday'
    END AS "Day of Week",
    hour_of_day AS "UTC Hour (0-23)",
    SUM(total_events_count) AS "Total Events",
    SUM(commit_events_count) AS "Commits",
    SUM(pr_events_count) AS "Pull Requests",
    SUM(distinct_human_developers) AS "Active Human Developers"
FROM iceberg.gold.mart_developer_work_patterns_daily
GROUP BY day_of_week, hour_of_day
ORDER BY day_of_week ASC, hour_of_day ASC
```

---

### Card 4: Programming Language Popularity & Activity Trends
* **Loại trực quan:** Biểu đồ đa đường (Multi-line Chart) hoặc Vùng xếp chồng (Stacked Area).
* **Mục đích:** Theo dõi xu hướng hoạt động phát triển phần mềm theo các ngôn ngữ lập trình chủ đạo qua từng ngày.
* **Bảng dữ liệu:** `iceberg.gold.mart_language_trends_daily`
* **Câu truy vấn SQL (Trino):**
```sql
SELECT 
    metric_date AS "Metric Date",
    primary_language AS "Programming Language",
    active_repos_count AS "Active Repositories",
    commits_count AS "Commits",
    opened_prs_count AS "Opened PRs",
    distinct_human_contributors AS "Human Contributors"
FROM iceberg.gold.mart_language_trends_daily
WHERE primary_language NOT IN ('unknown', 'null')
  AND primary_language IS NOT NULL
ORDER BY metric_date ASC, commits_count DESC
```

---

## 3. Xác Nhận Nguyên Tắc Thiết Kế (Gold Marts Only)
- Dashboard chỉ truy vấn vào schema `iceberg.gold` (và dữ liệu dimension người dùng đã chuẩn hóa ở `silver`).
- Không thực hiện quét trực tiếp dữ liệu thô `bronze.github_events` tại tầng BI nhằm tối ưu hóa chi phí I/O và đảm bảo dữ liệu hiển thị đã qua 100% Quality Gates.
