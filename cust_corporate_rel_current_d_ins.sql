WITH ins_lprel_tmp AS (
--產險法人集團客戶關聯人資料
SELECT
    CAST(DA501.ID_NO AS STRING) AS ID_NO, --統一編號
    'INS' AS SOURCE, --來源子公司別
    CAST(REL.RELATION_TYPE AS STRING) AS RELATION_TYPE, --關聯人種類
    CAST(REL.C_FULLNAME AS STRING) AS C_FULLNAME, --中文全名
    CAST(REL.REPRESNT_POS AS STRING) AS REPRESNT_POS, --職稱
    CAST(REL.RELATION_CODE AS STRING) AS RELATION_CODE, --與客戶關係
    CAST(REL.BIRTHDAY_DATE AS STRING) AS BIRTHDAY_DATE, --出生年月日
    CAST(REL.RELATION_ID_NO AS STRING) AS RELATION_ID_NO, --關聯人身分證號/ID/證件號
    CAST(REL.COUNTRY AS STRING) AS COUNTRY, --國籍
    CAST(CURRENT_DATE() AS TIMESTAMP_NTZ) AS LAST_MODIFIED_DATE,
    CAST('2024-08-16' AS TIMESTAMP_NTZ) AS CREATE_DATE --首次建立的日期
FROM (SELECT * FROM raw_clean.ins.DTATA501 WHERE DATE_FORMAT(DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date' ) AS DA501
--------業務員/理專、代表人、負責人、高管、實質受益人、具控制權人、被保人、受益人--------
INNER JOIN(
    -- 業務員關聯資料
    SELECT DISTINCT 
        DA501.CUSTOMER_ID AS ID,           -- 客戶ID
        1 AS RELATION_TYPE,                -- 關聯類型：1表示業務員
        NULL AS C_FULLNAME,                -- 客戶名稱
        NULL AS REPRESNT_POS,              -- 職稱
        NULL AS RELATION_CODE,             -- 與客戶關係
        NULL AS BIRTHDAY_DATE,             -- 出生年月日
        DP002.AGENT_ID AS RELATION_ID_NO,  -- 業務員ID
        NULL AS COUNTRY                    -- 國籍
    FROM raw_clean.ins.DTABP002 DP002
    LEFT JOIN raw_clean.ins.DTATA501 DA501
        ON DP002.CONTRACT_NO = DA501.CONTRACT_NO 
        AND DATE_FORMAT(DP002.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
        AND DATE_FORMAT(DA501.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
    UNION ALL
    -- 被保險人關聯資料
    SELECT DISTINCT 
        DA501.CUSTOMER_ID AS ID,           -- 客戶ID
        8 AS RELATION_TYPE,                -- 關聯類型：8表示被保險人
        DP004.CUSTOMER_NAME AS C_FULLNAME, -- 被保險人姓名
        NULL AS REPRESNT_POS,              -- 職稱
        DP004.RELATION_TYPE  AS RELATION_CODE,  -- 與客戶關係  --與自然人不同
        DP004.BIRTHDAY AS BIRTHDAY_DATE,   -- 出生年月日
        DP004.CUSTOMER_ID AS RELATION_ID_NO,-- 證件號
        DA501_NATIONAL.NATIONAL_COUNTRY_CODE AS COUNTRY -- 國籍
    FROM raw_clean.ins.DTABP004 DP004
    LEFT JOIN raw_clean.ins.DTATA501 DA501
        ON DP004.CONTRACT_NO = DA501.CONTRACT_NO 
        AND DATE_FORMAT(DP004.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
        AND DATE_FORMAT(DA501.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
    LEFT JOIN raw_clean.ins.DTATA501 DA501_NATIONAL
        ON DP004.CUSTOMER_ID = DA501_NATIONAL.CUSTOMER_ID
        AND DATE_FORMAT(DA501_NATIONAL.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
    UNION ALL
    -- 健傷險受益人關聯資料
    SELECT DISTINCT 
        DA501.CUSTOMER_ID AS ID,           -- 客戶ID
        9 AS RELATION_TYPE,                -- 關聯類型：9表示健傷險受益人
        DP606.ASSURED_NAME AS C_FULLNAME,  -- 受益人姓名
        NULL AS REPRESNT_POS,              -- 職稱
        DP606.RELATIVE_TO_INSURED AS RELATION_CODE, -- 與被保險人關係
        DA501_BIRTHDAY_NATIONAL.BIRTHDAY AS BIRTHDAY_DATE,   -- 出生年月日
        DP606.ASSURED_ID AS RELATION_ID_NO, -- 證件號
        DA501_BIRTHDAY_NATIONAL.NATIONAL_COUNTRY_CODE AS COUNTRY -- 國籍
    FROM raw_clean.ins.DTABP606 DP606
    LEFT JOIN raw_clean.ins.DTATA501 DA501
        ON DP606.CONTRACT_NO = DA501.CONTRACT_NO 
        AND DATE_FORMAT(DP606.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
        AND DATE_FORMAT(DA501.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
    LEFT JOIN raw_clean.ins.DTATA501 DA501_BIRTHDAY_NATIONAL
        ON DP606.ASSURED_ID = DA501_BIRTHDAY_NATIONAL.CUSTOMER_ID
        AND DATE_FORMAT(DA501_BIRTHDAY_NATIONAL.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
    UNION ALL
    -- 火險受益人關聯資料
    SELECT DISTINCT 
        DA501.CUSTOMER_ID AS ID,           -- 客戶ID
        9 AS RELATION_TYPE,                -- 關聯類型：9表示火險受益人
        DP205.ASSURED_NAME AS C_FULLNAME,  -- 受益人姓名
        NULL AS REPRESNT_POS,              -- 職稱
        NULL AS RELATION_CODE,             -- 與被保險人關係
        DA501_BIRTHDAY_NATIONAL.BIRTHDAY AS BIRTHDAY_DATE,   -- 出生年月日
        DP205.ASSURED_ID AS RELATION_ID_NO, -- 證件號
        DA501_BIRTHDAY_NATIONAL.NATIONAL_COUNTRY_CODE AS COUNTRY -- 國籍
    FROM raw_clean.ins.DTABP205 DP205
    LEFT JOIN raw_clean.ins.DTATA501 DA501
        ON DP205.CONTRACT_NO = DA501.CONTRACT_NO 
        AND DATE_FORMAT(DP205.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
        AND DATE_FORMAT(DA501.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
    LEFT JOIN raw_clean.ins.DTATA501 DA501_BIRTHDAY_NATIONAL
        ON DP205.ASSURED_ID = DA501_BIRTHDAY_NATIONAL.CUSTOMER_ID
        AND DATE_FORMAT(DA501_BIRTHDAY_NATIONAL.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
    UNION ALL
    -- 旅綜險受益人關聯資料
    SELECT DISTINCT 
        DA501.CUSTOMER_ID AS ID,           -- 客戶ID
        9 AS RELATION_TYPE,                -- 關聯類型：9表示旅綜險受益人
        DP703.ASSURED_NAME AS C_FULLNAME,  -- 受益人姓名
        NULL AS REPRESNT_POS,              -- 職稱
        DP703.RELATIVE_TO_INSURED AS RELATION_CODE, -- 與被保險人關係
        NULL AS BIRTHDAY_DATE,             -- 出生年月日
        NULL AS RELATION_ID_NO,            -- 證件號
        NULL AS COUNTRY                    -- 國籍
    FROM raw_clean.ins.DTPAP703 DP703
    LEFT JOIN raw_clean.ins.DTATA501 DA501
        ON DP703.CONTRACT_NO = DA501.CONTRACT_NO 
        AND DATE_FORMAT(DP703.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
        AND DATE_FORMAT(DA501.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
    UNION ALL
    -- 水險受益人關聯資料
    SELECT DISTINCT 
        DA501.CUSTOMER_ID AS ID,           -- 客戶ID
        9 AS RELATION_TYPE,                -- 關聯類型：9表示水險受益人
        COALESCE(DT310.BENEFIT_NAME, DT310.BENEFIT_NAME2) AS C_FULLNAME,  -- 受益人姓名
        NULL AS REPRESNT_POS,              -- 職稱
        NULL AS RELATION_CODE,             -- 與被保險人關係
        DA501_BIRTHDAY_NATIONAL.BIRTHDAY AS BIRTHDAY_DATE,   -- 出生年月日
        DT310.BENEFIT_NO AS RELATION_ID_NO, -- 證件號
        DA501_BIRTHDAY_NATIONAL.NATIONAL_COUNTRY_CODE AS COUNTRY -- 國籍
    FROM raw_clean.ins.DTATB310 DT310
    LEFT JOIN raw_clean.ins.DTATA501 DA501
        ON DT310.CONTRACT_NO = DA501.CONTRACT_NO 
        AND DATE_FORMAT(DT310.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
        AND DATE_FORMAT(DA501.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
    LEFT JOIN raw_clean.ins.DTATA501 DA501_BIRTHDAY_NATIONAL
        ON DT310.BENEFIT_NO = DA501_BIRTHDAY_NATIONAL.CUSTOMER_ID
        AND DATE_FORMAT(DA501_BIRTHDAY_NATIONAL.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
    UNION ALL
    -- 代表人關聯資料
    SELECT DISTINCT 
        DA501.CUSTOMER_ID AS ID,           -- 客戶ID
        2 AS RELATION_TYPE,                -- 關聯類型：2表示代表人
        DA525.MANAGER_NAME AS C_FULLNAME,  -- 代表人姓名
        DA525.RISK_JOBTITLE_CODE AS REPRESNT_POS, -- 職稱
        NULL AS RELATION_CODE,             -- 與客戶關係
        DA501_BIRTHDAY_NATIONAL.BIRTHDAY AS BIRTHDAY_DATE,   -- 出生年月日
        DA525.MANAGER_ID AS RELATION_ID_NO, -- 證件號
        DA501_BIRTHDAY_NATIONAL.NATIONAL_COUNTRY_CODE AS COUNTRY -- 國籍
    FROM raw_clean.ins.DTATA525 DA525
    LEFT JOIN raw_clean.ins.DTATA501 DA501
        ON DA525.CONTRACT_NO = DA501.CONTRACT_NO 
        AND DATE_FORMAT(DA525.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
        AND DATE_FORMAT(DA501.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
    LEFT JOIN raw_clean.ins.DTATA501 DA501_BIRTHDAY_NATIONAL
        ON DA525.MANAGER_ID = DA501_BIRTHDAY_NATIONAL.CUSTOMER_ID
        AND DATE_FORMAT(DA501_BIRTHDAY_NATIONAL.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
    UNION ALL
    -- 負責人關聯資料 --跟代表人一模一樣，後續可以嘗試用 cross join 合併
    SELECT DISTINCT 
        DA501.CUSTOMER_ID AS ID,           -- 客戶ID
        3 AS RELATION_TYPE,                -- 關聯類型：3表示負責人
        DA525.MANAGER_NAME AS C_FULLNAME,  -- 代表人姓名
        DA525.RISK_JOBTITLE_CODE AS REPRESNT_POS, -- 職稱
        NULL AS RELATION_CODE,             -- 與客戶關係
        DA501_BIRTHDAY_NATIONAL.BIRTHDAY AS BIRTHDAY_DATE,   -- 出生年月日
        DA525.MANAGER_ID AS RELATION_ID_NO, -- 證件號
        DA501_BIRTHDAY_NATIONAL.NATIONAL_COUNTRY_CODE AS COUNTRY -- 國籍
    FROM raw_clean.ins.DTATA525 DA525
    LEFT JOIN raw_clean.ins.DTATA501 DA501
        ON DA525.CONTRACT_NO = DA501.CONTRACT_NO 
        AND DATE_FORMAT(DA525.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
        AND DATE_FORMAT(DA501.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
    LEFT JOIN raw_clean.ins.DTATA501 DA501_BIRTHDAY_NATIONAL
        ON DA525.MANAGER_ID = DA501_BIRTHDAY_NATIONAL.CUSTOMER_ID
        AND DATE_FORMAT(DA501_BIRTHDAY_NATIONAL.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
) REL
ON DA501.CUSTOMER_ID = REL.ID
)

SELECT * FROM ins_lprel_tmp
WHERE ID_NO IS NOT NULL
AND SOURCE IS NOT NULL