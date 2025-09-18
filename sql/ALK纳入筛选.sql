-- 索引
create idx_ex on ex(subjid, excat, exchnam, exstdtc)
create idx_dg on dg(subjid, dgcat, dgterm, dgterm2, dgdtc)
create idx_tu on tu(subjid, tucat, tudtc)

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
    SELECT subjid, tudtc FROM tu where tucat = '早晚期信息' and tuorres = '晚期' and tudtc <> ''
),
-- NSCLC确诊后使用任意ALK抑制剂
nsclc_alk as (
    SELECT subjid, min_dgdtc FROM non_small_primary_patient t1 inner join alk_used t2 on t1.subjid = t2.subjid
    where exstdtc >= min_dgdtc
),
-- 首诊NSCLC后,有任意一次局晚期诊断
nsclc_min_stage as (
    SELECT t1.subjid, min(tudtc) min_tudtc FROM nsclc_alk t1 inner join mid_stage_patient t2 on t1.subjid = t2.subjid
    where tudtc >= min_dgdtc group by t1.subjid
),
-- 首诊NSCLC后,没有肺癌手术
nsclc_no_pr as (
    SELECT t3.subjid, min_tudtc FROM nsclc_min_stage t3 left join (
        SELECT t1.subjid FROM nsclc_min_stage t1 inner join pr t2 on t1.subjid = t2.subjid
        where prcat = '肺癌手术' and prdtc <> '' and prdtc >= min_tudtc
    ) t4 on t3.subjid = t4.subjid where t4.subjid is NULL
),
-- 首诊NSCLC后,有一次"晚期"记录
nsclc_last_stage as (
    SELECT t1.subjid, min(tudtc) min_tudtc FROM nsclc_alk t1 inner join last_stage_patient t2 on t1.subjid = t2.subjid
    where tudtc >= min_dgdtc group by t1.subjid
),
-- 确诊局晚期不可切+晚期转移性
nsclc_fianl_stage as (
    SELECT subjid, min(min_tudtc) indexdate FROM(
        SELECT subjid, min_tudtc FROM nsclc_no_pr
        union 
        SELECT subjid, min_tudtc FROM nscc_last_stage
    ) t1 group by subjid
),
-- indexdate在2019-06-01至2024-08-06,indexdate为首诊晚期时间
idx_19_24 as (
    SELECT subjid, indexdate FROM nsclc_fianl_stage where indexdate BETWEEN '2019-06-01' and '2024-08-06'
),
-- "晚期"后有任意一次ALK治疗
idx_alk_used as (
    SELECT subjid, indexdate FROM idx_19_24 t1 inner join alk_used t2 on t1.subjid = t2.subjid
    where exstdtc >= indexdate
),
-- 一线ALK治疗
alk_first_line as (
    SELECT t1.subjid, t1.indexdate FROM idx_alk_used t1 inner join first_line t2 on t1.subjid = t2.subjid
),
-- 排除同一天开两种以上药物的患者
alk_twice_used as (
    SELECT t1.subjid, t1.indexdate from alk_first_line t1 left join (
        SELECT subjid FROM alk_used group by subjid, exstdtc having count(DISTINCT exchnam) > 1
    ) t2 on t1.subjid = t2.subjid where t2.subjid is NULL
),
-- 年龄>= 18
alk_over_18 as (
    SELECT t1.subjid, t1.indexdate FROM alk_twice_used t1 inner join age_over_18 t2 on t1.subjid = t2.subjid
),
-- 排除合并疾病
exc_comorb as (
    SELECT t3.subjid, t3.indexdate FROM alk_over_18 t3 left join (
        SELECT t1.subjid FROM alk_over_18 t1 inner join dg t2 on t1.subjid = t2.subjid
        where dgdtc >= DATE_SUB(indexdate, INTERVAL 180 DAY) and indexdate > dgdtc
        and dgterm REGEXP '高血压|糖尿病|高血脂|血脂升高|血脂异常'
    ) t4 on t3.subjid = t4.subjid where t4.subjid is NULL
)
-- 化疗、免疫、靶向随机组合查询

-- indexdate的年份分布

-- 一线治疗，排除药物开始时间在indexdate之前的患者

-- 探查:indexdate前用药-手术相关

-- 探查:indexdate前用药-ALK探测相关

