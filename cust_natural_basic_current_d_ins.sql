WITH ins_npbasic_tmp AS (
--自然人集團客戶基本資料
SELECT
    CAST(CASE
        WHEN SUBSTRING(DA501.CUSTOMER_ID,1,2) RLIKE '[A-Z][1-2]'
        THEN 1
        WHEN SUBSTRING(DA501.CUSTOMER_ID,1,2) RLIKE '[A-Z][8-9]'
        THEN 2
        WHEN SUBSTRING(DA501.CUSTOMER_ID,1,2) RLIKE '[A-Z][A-Z]'
        THEN 2
        ELSE 3
    END AS STRING) AS ID_TYPE, --主證號 / ID類型  --看到實體資料再做調整
    CAST(DA501.CUSTOMER_ID AS STRING) AS ID_NO, --自然人證照號碼
    'INS' AS SOURCE, --來源子公司別
    CAST(DA501.CUSTOMER_NAME AS STRING) AS C_FULLNAME, --中文姓名
    CAST(DA501.CUSTOMER_ENGLISH_NAME AS STRING) AS E_FULLNAME, --英文姓名
    CAST(DA501.SEX AS STRING) AS GENDER, --性別
    CAST(DA501.BIRTHDAY AS STRING) AS BIRTHDAY_DATE, --出生年月日
    '' AS EXPIRED_DATE, --居留證到期日
    '' AS PASSPORT_NUMBER, --護照號碼
    '' AS PASSPORT_VALIDITY, --護照號碼到期日
    CAST(DA501.NATIONAL_COUNTRY_CODE AS STRING) AS COUNTRY, --國籍
    CAST(REGISTERED_ADDR.ADDRESS AS STRING) AS REGISTERED_ADDR, --戶籍地址
    '' AS O_NAME, --別名
    CAST(DA501.SCHOOL_DEGREE AS STRING) AS EDUCATION_CODE,--教育程度
    CAST(DA501.OCCU_TYPE1_CODE AS STRING) AS INDUSTRY_TYPE1,--行職業大項
    CAST(DA501.OCCU_TYPE3_CODE AS STRING) AS INDUSTRY_TYPE2,--行職業小項
    CAST(DA501.DIV_NAME AS STRING) AS SERVICES,--任職機構
    CAST(DA501.RISK_JOBTITLE_CODE AS STRING) AS INDUSTRY_TITLE,--職稱
    CAST(INVEST_SOURCE.PAY_SOURCE AS STRING) AS INVEST_SOURCE,--資產來源
    '' AS ESTATE_SOURCE,--資金來源
    CAST(ANNUAL_INCOME.MIN_YEAR_INCOME AS STRING) AS ANNUAL_INCOME,--年收入
    '' AS PRODUCT_CUB,--建立業務目的
    CAST(CURRENT_DATE() AS TIMESTAMP_NTZ) AS LAST_MODIFIED_DATE,
    CAST('2024-08-16' AS TIMESTAMP_NTZ) AS CREATE_DATE--這裡要改成首次建立的日期
FROM (SELECT * FROM raw_clean.ins.DTATA501 WHERE DATE_FORMAT(DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date' ) AS DA501

----------戶籍地址----------
LEFT JOIN (
    SELECT * FROM (
        SELECT ID, ADDRESS, ROW_NUMBER() OVER (PARTITION BY ID ORDER BY CREATE_DATE DESC) sn FROM (
            SELECT CUSTOMER_ID AS ID, ADDRESS, CREATE_DATE
            FROM raw_clean.ins.DTABP005
            WHERE ADDRESS_KIND = '1'
            AND ADDRESS <> ''
            AND ADDRESS IS NOT NULL
            AND DATE_FORMAT(DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
        ) r
    ) R
    WHERE R.sn = 1
) REGISTERED_ADDR
ON DA501.CUSTOMER_ID = REGISTERED_ADDR.ID

----------資產來源----------
LEFT JOIN (
    SELECT * FROM (
        SELECT ID, PAY_SOURCE, ROW_NUMBER() OVER (PARTITION BY ID ORDER BY CREATE_DATE DESC) sn FROM (
            SELECT DA501.CUSTOMER_ID AS ID, DP707.PAY_SOURCE, DP707.CREATE_DATE_TIME AS CREATE_DATE
            FROM raw_clean.ins.DTPAP707 DP707
            LEFT JOIN raw_clean.ins.DTATA501 DA501
            ON DP707.CONTRACT_NO = DA501.CONTRACT_NO
            AND DATE_FORMAT(DP707.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
            AND DATE_FORMAT(DA501.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
            WHERE DP707.PAY_SOURCE <> ''
            AND DP707.PAY_SOURCE IS NOT NULL
            UNION ALL
            SELECT DA501.CUSTOMER_ID AS ID, DP611.PREMIUM_SOURCE AS PAY_SOURCE, DP611.CREATE_DATE AS CREATE_DATE
            FROM raw_clean.ins.DTABP611 DP611
            LEFT JOIN raw_clean.ins.DTATA501 DA501
            ON DP611.CONTRACT_NO = DA501.CONTRACT_NO
            AND DATE_FORMAT(DP611.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
            AND DATE_FORMAT(DA501.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
            WHERE DP611.PREMIUM_SOURCE <> ''
            AND DP611.PREMIUM_SOURCE IS NOT NULL
        ) r
    ) R
    WHERE R.sn = 1
) INVEST_SOURCE
ON DA501.CUSTOMER_ID = INVEST_SOURCE.ID

----------年收入----------
LEFT JOIN (
    SELECT * FROM (
        SELECT ID, MIN_YEAR_INCOME, ROW_NUMBER() OVER (PARTITION BY ID ORDER BY CREATE_DATE DESC) sn FROM (
            SELECT DA501.CUSTOMER_ID AS ID, DP707.MIN_YEAR_INCOME, DP707.CREATE_DATE_TIME AS CREATE_DATE
            FROM raw_clean.ins.DTPAP707 DP707
            LEFT JOIN raw_clean.ins.DTATA501 DA501
            ON DP707.CONTRACT_NO = DA501.CONTRACT_NO
            AND DATE_FORMAT(DP707.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
            AND DATE_FORMAT(DA501.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
            WHERE DP707.MIN_YEAR_INCOME <> ''
            AND DP707.MIN_YEAR_INCOME IS NOT NULL
            UNION ALL
            SELECT DA501.CUSTOMER_ID AS ID, DP611.MIN_YEAR_INCOME, DP611.CREATE_DATE AS CREATE_DATE
            FROM raw_clean.ins.DTABP611 DP611
            LEFT JOIN raw_clean.ins.DTATA501 DA501
            ON DP611.CONTRACT_NO = DA501.CONTRACT_NO
            AND DATE_FORMAT(DP611.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
            AND DATE_FORMAT(DA501.DBC_BUSINESS_DATE, 'yyyy-MM-dd') = '$business_date'
            WHERE DP611.MIN_YEAR_INCOME <> ''
            AND DP611.MIN_YEAR_INCOME IS NOT NULL
        ) r
    ) R
    WHERE R.sn = 1
) ANNUAL_INCOME
ON DA501.CUSTOMER_ID = ANNUAL_INCOME.ID

)
