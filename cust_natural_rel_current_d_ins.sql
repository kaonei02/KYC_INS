WITH  ins_nprel_tmp AS (
--自然人集團客戶關聯人資料
SELECT
    CAST(CASE
        WHEN SUBSTRING(DA501.ID_NO,1,2) RLIKE '[A-Z][1-2]'
        THEN 1
        WHEN SUBSTRING(DA501.ID_NO,1,2) RLIKE '[A-Z][8-9]'
        THEN 2
        WHEN SUBSTRING(DA501.ID_NO,1,2) RLIKE '[A-Z][A-Z]'
        THEN 2
        ELSE 3
    END AS STRING) AS ID_TYPE, --主證號 / ID類型
    CAST(DA501.ID_NO AS STRING) AS ID_NO, --自然人證照號碼
    'INS' AS SOURCE, --來源子公司別
    CAST(REL.RELATION_TYPE AS STRING) AS RELATION_TYPE,--關聯人種類
    CAST(REL.C_FULLNAME AS STRING) AS C_FULLNAME,--中文全名
    CAST(REL.RELATION_CODE AS STRING) AS RELATION_CODE,--與客戶關係
    CAST(REL.BIRTHDAY_DATE AS STRING) AS BIRTHDAY_DATE,--出生年月日
    CAST(REL.RELATION_ID_NO AS STRING) AS RELATION_ID_NO,--關聯人身分證號/ID/證件號
    CAST(REL.COUNTRY AS STRING) AS COUNTRY,--國籍
    CAST(CURRENT_DATE() AS TIMESTAMP_NTZ) AS LAST_MODIFIED_DATE,
    CAST('2024-08-16' AS TIMESTAMP_NTZ) AS CREATE_DATE,--這裡要改成首次建立的日期
    CAST(CASE 
        WHEN SUBSTRING(DA501.BIRTHDAY,4,1) RLIKE '[0-9]' THEN SUBSTRING(DA501.BIRTHDAY,4,1)
        ELSE 0
    END AS STRING) AS PTN
FROM (SELECT * FROM raw_clean.ins.DTATA501 WHERE DATE_FORMAT(DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date' ) AS DA501
--------業務員/理專、法定代理人、被保人、受益人--------
INNER JOIN(
    -- 業務員關聯資料
    -- 從DTABP002(業務員資料)和DTATA501(客戶資料)取得業務員關聯資訊
    SELECT DISTINCT 
        DA501.CUSTOMER_ID AS ID,           -- 客戶ID
        1 AS RELATION_TYPE,                -- 關聯類型：1表示業務員
        NULL AS C_FULLNAME,                -- 客戶名稱
        NULL AS RELATION_CODE,             -- 與客戶關係(業務員關聯不需要)
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
    -- 從DTABP004(被保險人資料)和DTATA501(客戶資料)取得被保險人關聯資訊
    SELECT DISTINCT 
        DA501.CUSTOMER_ID AS ID,           -- 客戶ID
        4 AS RELATION_TYPE,                -- 關聯類型：4表示被保險人
        DP004.CUSTOMER_NAME AS C_FULLNAME, -- 被保險人姓名
        NULL AS RELATION_CODE,             -- 與客戶關係(被保險人關聯不需要)
        DP004.BIRTHDAY AS BIRTHDAY_DATE,   -- 出生年月日
        DP004.CUSTOMER_ID AS RELATION_ID_NO,-- 證件號
        DA501_NATIONAL.NATIONAL_COUNTRY_CODE AS COUNTRY -- 國籍(從DA501取得)
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
    -- 從DTABP606(健傷險受益人資料)和DTATA501(客戶資料)取得受益人關聯資訊
    SELECT DISTINCT 
        DA501.CUSTOMER_ID AS ID,           -- 客戶ID
        5 AS RELATION_TYPE,                -- 關聯類型：5表示受益人
        DP606.ASSURED_NAME AS C_FULLNAME,  -- 受益人姓名
        DP606.RELATIVE_TO_INSURED AS RELATION_CODE, -- 與被保險人關係
        DA501_BIRTHDAY_NATIONAL.BIRTHDAY AS BIRTHDAY_DATE,   -- 出生年月日(從DA501取得)
        DP606.ASSURED_ID AS RELATION_ID_NO, -- 證件號
        DA501_BIRTHDAY_NATIONAL.NATIONAL_COUNTRY_CODE AS COUNTRY -- 國籍(從DA501取得)
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
    -- 從DTABP205(火險受益人資料)和DTATA501(客戶資料)取得受益人關聯資訊
    SELECT DISTINCT 
        DA501.CUSTOMER_ID AS ID,           -- 客戶ID
        5 AS RELATION_TYPE,                -- 關聯類型：5表示受益人
        DP205.ASSURED_NAME AS C_FULLNAME,  -- 受益人姓名
        NULL AS RELATION_CODE,             -- 與被保險人關係(火險受益人不需要)
        DA501_BIRTHDAY_NATIONAL.BIRTHDAY AS BIRTHDAY_DATE,   -- 出生年月日(從DA501取得)
        DP205.ASSURED_ID AS RELATION_ID_NO, -- 證件號
        DA501_BIRTHDAY_NATIONAL.NATIONAL_COUNTRY_CODE AS COUNTRY -- 國籍(從DA501取得)
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
    -- 從DTPAP703(旅綜險受益人資料)和DTATA501(客戶資料)取得受益人關聯資訊
    SELECT DISTINCT 
        DA501.CUSTOMER_ID AS ID,           -- 客戶ID
        5 AS RELATION_TYPE,                -- 關聯類型：5表示受益人
        DP703.ASSURED_NAME AS C_FULLNAME,  -- 受益人姓名
        DP703.RELATIVE_TO_INSURED AS RELATION_CODE, -- 與被保險人關係
        NULL AS BIRTHDAY_DATE,             -- 出生年月日(無法取得)
        NULL AS RELATION_ID_NO,            -- 證件號(無法取得)
        NULL AS COUNTRY                    -- 國籍(無法取得)
    FROM raw_clean.ins.DTPAP703 DP703
    LEFT JOIN raw_clean.ins.DTATA501 DA501
        ON DP703.CONTRACT_NO = DA501.CONTRACT_NO 
        AND DATE_FORMAT(DP703.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
        AND DATE_FORMAT(DA501.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
    UNION ALL
    -- 水險受益人關聯資料
    -- 從DTATB310(水險受益人資料)和DTATA501(客戶資料)取得受益人關聯資訊
    SELECT DISTINCT 
        DA501.CUSTOMER_ID AS ID,           -- 客戶ID
        5 AS RELATION_TYPE,                -- 關聯類型：5表示受益人
        COALESCE(DT310.BENEFIT_NAME, DT310.BENEFIT_NAME2) AS C_FULLNAME,  -- 受益人姓名
        NULL AS RELATION_CODE,             -- 與被保險人關係(水險受益人不需要)
        DA501_BIRTHDAY_NATIONAL.BIRTHDAY AS BIRTHDAY_DATE,   -- 出生年月日(從DA501取得)
        DT310.BENEFIT_NO AS RELATION_ID_NO, -- 證件號
        DA501_BIRTHDAY_NATIONAL.NATIONAL_COUNTRY_CODE AS COUNTRY -- 國籍(從DA501取得)
    FROM raw_clean.ins.DTATB310 DT310
    LEFT JOIN raw_clean.ins.DTATA501 DA501
        ON DT310.CONTRACT_NO = DA501.CONTRACT_NO 
        AND DATE_FORMAT(DT310.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
        AND DATE_FORMAT(DA501.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
    LEFT JOIN raw_clean.ins.DTATA501 DA501_BIRTHDAY_NATIONAL
        ON DT310.BENEFIT_NO = DA501_BIRTHDAY_NATIONAL.CUSTOMER_ID
        AND DATE_FORMAT(DA501_BIRTHDAY_NATIONAL.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
) REL
ON DA501.CUSTOMER_ID = REL.ID
)