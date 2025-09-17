-- 索引
create idx_dg on dg(subjid, dgterm, dgcode, dgdtc)
create idx_ex on ex(subjid, exchnam, exstdtc)

-- 确诊克罗恩病、溃疡性结肠炎、IBD
with A as (
    SELECT subjid, umrid, dgdtc, dgterm, dgcode, ibdyn, dgcat, dglevel, dgimm FROM dg where dgcode REGEXP 'K50|K51' 
    or dgterm REGEXP '克罗恩病|克隆氏(病|症)|克隆氏症候群|局(部|限).?肠炎|局限.?肠炎|节段.?肠炎|肉芽肿.?肠炎|溃疡.?结肠炎|未定.?结肠炎|炎症.?肠病'
    and dgterm not REGEXP '疑似|可疑|存疑|可能|推测|多为|待排|待查|鉴别|考虑|不排除'
),
-- IBD诊断确定
B as (
    SELECT subjid, umrid, dgdtc, dgterm, dgcode, ibdyn, dgcat, dglevel, dgimm FROM A where ibdyn = '是'
),
-- 首次诊断日期在2019.6.1至2025.6.30
C as (
    SELECT subjid, umrid, dgdtc, dgterm, dgcode, ibdyn, dgcat, dglevel, dgimm, rk FROM (
        SELECT subjid, umrid, dgdtc, dgterm, dgcode, ibdyn, dgcat, dglevel, dgimm, rank() over(PARTITION BY subjid order by dgdtc) rk FROM B
    ) t1 where rk = 1 and dgdtc BETWEEN '2019-06-01' and '2025-06-30'
),
-- 首次确诊UC或CD
D as (
    SELECT subjid, umrid, dgdtc first_dgdtc, dgterm, dgcode, ibdyn, dgcat, dglevel, dgimm, rk FROM C where dgcat REGEXP 'UC|CD'
),
-- 排除自免疾病
E as (
    SELECT D.subjid, D.first_dgdtc, D.dgcat FROM D LEFT JOIN (
        SELECT DISTINCT subjid FROM dg where dgimm <> ''
    ) t1 on D.subjid = t1.subjid
    where t1.subjid is NULL
),
-- 判断诊断是否是中重度
F as (
    SELECT t1.subjid, first_dgdtc, dgcat FROM E t1 INNER JOIN ex t2 on t1.subjid = t2.subjid 
    where t2.exchnam <> '' and t2.exstdtc >= DATE_SUB(first_dgdtc, INTERVAL 30 DAY) and t2.exstdtc <= '2025-06-30'
    UNION
    SELECT t3.subjid, first_dgdtc, dgcat FROM E t3 INNER JOIN dg t4 on t3.subjid = t4.subjid 
    where t4.dglevel = '中重度' and t4.dgdtc BETWEEN '2019-06-01' and '2025-06-30'
),
-- 2019.12.1至2024.6.30内，患者的index date和使用品牌
G as (
    SELECT subjid, first_dgdtc, dgcat, exstdtc indexdate, exchnam FROM (
        SELECT subjid, first_dgdtc, dgcat, exstdtc, exchnam, rk, rank() over(PARTITION BY subjid order by subjid) rk1 FROM (
            SELECT t1.subjid, t1.first_dgdtc, t1.dgcat, t2.exstdtc, t2.exchnam rank() over(
                PARTITION BY t1.subjid,t2.exchnam order by t2.exstdtc
            ) rk FROM F t1 INNER JOIN ex t2 on t1.subjid = t2.subjid where exchnam REGEXP '维得利珠|阿达木|英夫利西|乌司奴|乌帕替尼'
            AND exstdtc >= DATE_SUB(first_dgdtc, INTERVAL 30 DAY)
        ) t3 where rk = 1 and exstdtc BETWEEN '2019-12-01' and '2024-06-30'
    ) t4 where rk1 = 1
),
-- 年龄>=18
H as (
    SELECT subjid, first_dgdtc, dgcat, indexdate, exchnam, datediff(indexdate, brthdtc)/365.25 age FROM G t1 INNER JOIN dm t2 on t2.subjid = t2.subjid
    where brthdtc <= DATE_SUB(indexdate, INTERVAL 18 YEAR)
)