# 数据字典

| 字段 | 类型 | 可空 | 定义 |
|---|---|---:|---|
| `run_id` | string | 否 | 输入摘要派生的稳定运行编号 |
| `source_line` | integer | 否 | 原文件物理行号 |
| `reason_code` | string | 是 | 隔离或冲突的稳定原因码 |
| `raw_value` | string | 是 | 未归一的原始证据 |
| `normalized_value` | string | 是 | 按项目规则得到的值 |
| `rule_version` | string | 否 | 计算规则版本 |

字符串编号逐字保留。金额与计量值用 `Decimal`。时间值带偏移并以 UTC 参与比较。区间统一采用左闭右开。

## M7 离线分支合并

| 字段 | 类型 | 可空 | 定义 |
|---|---|---:|---|
| `branch_id` | string | 否 | 站点在指定主库快照上派生的分支编号 |
| `seal_id` | string | 否 | 分支封账回执编号，一经生成不可变 |
| `seal_receipt.buckets` | array | 否 | 该站点按自身实际日志算出的原结果桶值 |
| `merge_id` | string | 否 | 一次合并尝试的编号，同一封账组合重试复用 |
| `base_version` | integer | 否 | 分支派生时的主库版本 |
| `expected_version` | integer | 否 | 调用方认定的主库当前版本；不符即拒绝 |
| `action` | string | 否 | `INSERT` 新请求、`REPLACE` 覆盖同请求旧值 |
| `held` | integer | 否 | 1 表示所在桶有未裁决冲突，暂不写入 |
| `conflict_id` | string | 是 | 冲突编号，由合并编号与请求编号派生 |
| `side` | string | 否 | 候选来源：`main` 或分支编号 |
| `content_hash` | string | 否 | 事件内容指纹（不含来源行号） |
| `reason_code` | string | 否 | 冲突原因码，本模块为 `DUPLICATE_CONFLICT` |
| `resolution_reason` | string | 是 | 裁决留下的原因，必填 |
| `state` | string | 否 | 桶差异状态：`APPLIED` 或 `HELD` |
| `delta_count` | integer | 否 | 桶计数相对合并前的增量，不做两地相加 |
