-- 一线治疗的患者
with first_line as (
    SELECT subjid FROM ex where excat = '肿瘤药物治疗' and extype = '一线治疗' and exchnam REGEXP '阿来替尼|洛拉替尼|恩沙替尼'
),
-- 使用药物治疗的患者
alk_used as (
    SELECT subjid, umrid, exstdtc, exchnam, extrtcat FROM ex where excat = '肿瘤药物治疗' and exchnam REGEXP '阿来替尼|洛拉替尼|恩沙替尼'
),
-- 原发患者
primary_patient as (
    SELECT subjid, min(dgdtc) first_dgdtc FROM dg where dgcat = '肺癌诊断信息' and dgloc3 = '原发' GROUP BY subjid
),
-- 非小细胞患者
non_small_patient as (
    SELECT subjid FROM dg where dgcat = '病理诊断' and (dgterm2 <> '小细胞肺癌' and dgterm2 <> '小细胞肺癌-分期衍生' and dgterm2 <> '')
),
-- 年龄大于18岁的患者
age_over_18 as (
    SELECT subjid FROM dm where cast(age as INTEGER) >= 18
),
-- 原发非小患者
non_small_primary_patient as (
    SELECT t1.subjid, 
    case when t3.first_exstdtc is null then t1.first_dgdtc
        when t1.first_dgdtc > t3.first_exstdtc then t3.first_exstdtc
        else t1.first_dgdtc end min_dgdtc
    FROM primary_patient t1 
    inner join non_small_patient t2 on t1.subjid = t2.subjid
    left join (SELECT subjid, min(exstdtc) first_exstdtc from alk_used group by subjid) t3 on t2.subjid = t3.subjid
),
-- 诊断为局晚期的患者
mid_stage_patient as (
    SELECT subjid, tudtc FROM tu where tucat = '早晚期信息' and tuorres = '局晚期' and tudtc <> ''
),
-- 诊断为局晚期的患者
last_stage_patient as (
    SELECT * FROM tu where tucat = '早晚期信息' and tuorres = '晚期' and tudtc <> '' 
),
-- NSCLC确诊后使用任意ALK抑制剂
nsclc_alk as (
    SELECT subjid FROM non_small_primary_patient t1 inner join alk_used t2 on t1.subjid = t2.subjid
    where exstdtc >= min_dgdtc
),
-- 首诊NSCLC后,没有肺癌手术

-- 首诊NSCLC后,有一次"晚期"记录

-- 确诊局晚期不可切+晚期转移性

-- indexdate在2019-06-01至2024-08-06,indexdate为首诊晚期时间

-- "晚期"后有任意一次ALK治疗

-- 一线ALK治疗

-- 排除同一天开两种以上药物的患者

-- 年龄>= 18

-- 排除合并疾病

-- 化疗、免疫、靶向随机组合查询

-- indexdate的年份分布

-- 一线治疗，排除药物开始时间在indexdate之前的患者

-- 探查:indexdate前用药-手术相关

-- 探查:indexdate前用药-ALK探测相关

