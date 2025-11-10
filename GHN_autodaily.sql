WITH Detail AS (
  SELECT
    C.fromregionshortname AS Region,
    fromprovince AS Province,
    fromwardcode AS Ward_id,
    fromdistrict AS District,
    DATE(C.orderdate) AS Order_Date,
    -- Tính toán OntimeSuccessPU
    CASE
      WHEN (
        HOUR(C.orderdate) < 18 AND DATE(C.endpicktime) <= DATE(C.orderdate)
      ) OR (
        HOUR(C.orderdate) >= 18 AND COALESCE(DATE(C.endpicktime), CURRENT_DATE) <= DATE(C.orderdate + INTERVAL '1' DAY)
      )
      THEN 1
      ELSE 0
    END AS OntimeSuccessPU,
    
    -- Tính toán OntimeFirstPU (Logic chi tiết của bạn)
    CASE
  -- Ưu tiên 1: endpicktime <= orderdate (Thành công ngay trong ngày D)
  WHEN endpicktime IS NOT NULL AND DATE(endpicktime) <= DATE(orderdate) THEN 1

  -- Ưu tiên 2: Giờ đặt < 18h (Deadline: cuối ngày D)
  WHEN HOUR(orderdate) < 18 AND (
    (
      COALESCE(firstfailpicknote, '') = 'Nhân viên gặp sự cố' AND
      secondupdatedpickeduptime IS NOT NULL AND DATE(secondupdatedpickeduptime) <= DATE(orderdate)
    )
    OR (
      COALESCE(firstfailpicknote, '') != 'Nhân viên gặp sự cố' AND
      COALESCE(firstupdatedpickeduptime, secondupdatedpickeduptime) IS NOT NULL AND
      DATE(COALESCE(firstupdatedpickeduptime, secondupdatedpickeduptime)) <= DATE(orderdate)
    )
  ) THEN 1

  -- Ưu tiên 3: Giờ đặt >= 18h (Deadline: cuối ngày D + 1)
  WHEN HOUR(orderdate) >= 18 AND (
    (
      COALESCE(firstfailpicknote, '') = 'Nhân viên gặp sự cố' AND
      secondupdatedpickeduptime IS NOT NULL AND DATE(secondupdatedpickeduptime) <= DATE(orderdate + INTERVAL '1' DAY)
    )
    OR (
      COALESCE(firstfailpicknote, '') != 'Nhân viên gặp sự cố' AND
      COALESCE(firstupdatedpickeduptime, secondupdatedpickeduptime) IS NOT NULL AND
      DATE(COALESCE(firstupdatedpickeduptime, secondupdatedpickeduptime)) <= DATE(orderdate + INTERVAL '1' DAY)
    )
  ) THEN 1

  -- Các trường hợp còn lại
  ELSE 0
END AS OntimeFirstPU

  FROM dtm_ka_v3_createddate C
  WHERE
    C.clientid IN (18692)
    AND isexpecteddropoff = FALSE
    -- Thiết lập phạm vi dữ liệu rộng nhất (đầu tháng trước đến hôm qua) để bao gồm mọi view
    AND DATE(C.orderdate) >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' MONTH)
    AND DATE(C.orderdate) <= (CURRENT_DATE - INTERVAL '1' DAY)
    AND C.channel != 'WH - Shopee'
    AND c.currentstatus != 'cancel'
)

---
-- 1. View Daily: 10 Ngày Gần Nhất (Region và Total)
SELECT
  'Daily' AS Time_Type,
  CAST(Order_Date AS VARCHAR) AS Time_Period,
  COALESCE(Region, 'Nationwide Total') AS Region,
  ROUND(SUM(OntimeSuccessPU) * 1.0000 / COUNT(*), 4) AS "%OntimeSuccessPU",
  ROUND(SUM(OntimeFirstPU) * 1.0000 / COUNT(*), 4) AS "%Ontime1stPU"
FROM Detail
WHERE
    Order_Date BETWEEN (CURRENT_DATE - INTERVAL '10' DAY) AND (CURRENT_DATE - INTERVAL '1' DAY)
GROUP BY GROUPING SETS (
    (Order_Date, Region),
    (Order_Date)
)

UNION ALL

---
-- 2. View Weekly: Tuần Trước (Region và Total)
-- Giả định tuần bắt đầu từ Thứ Hai (Monday)
SELECT
  'Weekly' AS Time_Type,
  CAST(Time_Key AS VARCHAR) AS Time_Period,
  COALESCE(Region, 'Nationwide Total') AS Region,
  ROUND(SUM(OntimeSuccessPU) * 1.0000 / COUNT(*), 4) AS "%OntimeSuccessPU",
  ROUND(SUM(OntimeFirstPU) * 1.0000 / COUNT(*), 4) AS "%Ontime1stPU"
FROM (
    -- Subquery để định nghĩa Time_Key là Ngày bắt đầu của Tuần
    SELECT
        DATE_TRUNC('week', Order_Date) AS Time_Key, -- Tuỳ thuộc vào cấu hình Trino (Thường là Chủ Nhật hoặc Thứ Hai)
        Region,
        OntimeSuccessPU,
        OntimeFirstPU
    FROM Detail
    WHERE
        -- Lọc cho Tuần trước (7 ngày kết thúc vào Chủ Nhật)
        Order_Date >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '7' DAY
        AND Order_Date <= CURRENT_DATE - INTERVAL '1' DAY
) AS WeeklyData
GROUP BY GROUPING SETS (
    (Time_Key, Region),
    (Time_Key)
)

UNION ALL

---
--## 3. View Monthly: Tháng Hiện Tại & Tháng Liền Kề Trước Đó (Region và Total)
SELECT
  'Monthly' AS Time_Type,
  CAST(Time_Key AS VARCHAR) AS Time_Period,
  COALESCE(Region, 'Nationwide Total') AS Region,
  ROUND(SUM(OntimeSuccessPU) * 1.0000 / COUNT(*), 4) AS "%OntimeSuccessPU",
  ROUND(SUM(OntimeFirstPU) * 1.0000 / COUNT(*), 4) AS "%Ontime1stPU"
FROM (
    -- Subquery để tạo alias Time_Key từ DATE_TRUNC('month', Order_Date)
    SELECT
        DATE_TRUNC('month', Order_Date) AS Time_Key,
        Region,
        OntimeSuccessPU,
        OntimeFirstPU
    FROM Detail
    WHERE
        Order_Date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' MONTH)
) AS MonthlyData
GROUP BY GROUPING SETS (
    (Time_Key, Region), -- Chỉ sử dụng alias Time_Key
    (Time_Key)
)
ORDER BY Time_Type, Time_Period DESC, Region DESC;
