# 1. 编辑 docs/医疗数据治理/面试百问百答.md，更新顶部版本号

# 2. 提交更改

git add docs/医疗数据治理/面试百问百答.md
git commit -m "feat: 新增 Q11/Q12 [V1.1]"

# 3. 打标签

git tag -a v1.1 -m "新增脱敏策略、血缘审计用例"

# 4. 推送

git push origin main
git push origin v1.1

# 5. 验证

git tag                    # 查看本地标签
git show v1.1              # 查看标签详情
