# P0 实施设计:多采样共识打分 + 画像职责厘清

> 关联 roadmap: `.omc/plans/business-optimization-roadmap.md`
> 关联正确性 bug: `.omc/plans/fitting-token-burn-fix.md`(独立,本设计不含)

## 0. 读码修正(重要前提)
设计前重读代码，纠正了 roadmap 里一个错误假设:

- **`candidate_summary` 已经是 profile 维度固化的静态配置**,来自
  `profile_record["candidate_summary_config"]`(`fitting_notifier.py:1646`
  `_load_candidate_summary_config`),**不是模型每次生成的**。
- `_apply_fit_caps`(`fitting_notifier.py:855`)里的硬阻断判定
  (`candidate_years`/`candidate_seniority`/`seniority_gap`/`gap_years`)
  **全部基于这份固定配置**,模型输出的 `candidate_summary` 字段
  在 `:981` 被**直接覆盖**为固定配置,根本不参与判定。
- **结论:P0.2「画像漂移」问题不存在**。画像已经固化。
  P0.2 退化为"prompt 里去掉让模型重复产出 candidate_summary 的冗余指令"
  (纯优化，低优先级，非正确性问题)。
- **真正的主菜是 P0.1 多采样共识**,本设计聚焦于此。

### 0.1 Codex 复审修订(给 Claude)

Codex 对本设计做了二次读码和风险复审。总体判断: **P0.1 值得做,但不能按原稿
直接实现**。首轮应只做"显式启用的多采样共识 + 保守聚合 + 可观测字段",避免把
prompt、前端、token-burn、跨模型路由一起混入。

必须修订点:

1. **默认值不能改变成本/延迟行为**  
   原稿写 `FITTING_SAMPLE_COUNT` 默认 `3`,但这不是真正向后兼容:无配置升级会把
   LLM 调用数、延迟、限流风险默认放大约 3 倍。建议默认 `1`;只有显式配置
   `>1` 才启用多采样。

2. **原稿"中位数=保守"不成立**  
   对默认 3 样本,`[Strong, Strong, Not Recommended]` 的 decision 中位数仍是
   `Strong`,与"prefer false negative"目标和后文测试用例冲突。实现前必须改成
   明确的保守聚合/分歧 veto,不能直接用普通中位数作为最终决策。

3. **首轮不混做 multi-endpoint**  
   当前 `_request_llm_json_with_fallback` 每次从第一个 endpoint 开始。若要真正跨模型
   采样,需要 endpoint 轮换、effective model 去重、fallback 顺序、样本模型记录等额外
   设计。建议首 PR 先做 `single_endpoint` 重复采样;`multi_endpoint/auto` 放 Step 2。

4. **全失败时必须保留现有 RuntimeError 语义**  
   如果所有样本都是 transient API error,应继续抛 `TRANSIENT_API::...` 让现有队列
   requeue;fatal 仍走 `FATAL_API::...`。不要把 API 全失败静默降级成普通
   `llm_match_error`,否则会改变队列/重试语义。

5. **不在本轮做 P0.2、Django 前端、token-burn fix**  
   P0.2 是低收益 prompt cleanup;Django 展示不在本仓;token-burn 是独立 correctness bug。
   这些都不应和 P0.1 首轮混在一个 PR 里。

---

## 1. P0.1 多采样共识打分

### 1.1 目标
单个 job 从"单次 LLM 调用 → 单点分数"升级为"N 次采样 → 共识分数 + 分歧度"，
方向严格保守(prefer false negative),并把分歧度落库供前端标注灰色地带。

### 1.2 配置(全部向后兼容)
新增 env,读取走现有 `runtime_int`/`os.getenv`:
- `FITTING_SAMPLE_COUNT`(默认 `1`;只有显式设为 `>1` 才启用多采样)
- `FITTING_SAMPLE_MODE`(`single_endpoint`|`multi_endpoint`,默认 `single_endpoint`)
  - **首 PR 只实现/启用 `single_endpoint`**:同一首选 endpoint 重复采样 N 次,保留现有
    fallback 语义。
  - `multi_endpoint` 放 Step 2;实现前需明确 effective model 去重、endpoint 轮换、失败
    记录和 `model_name` 汇总。
- `FITTING_SAMPLE_TEMPERATURE`(仅 `FITTING_SAMPLE_COUNT>1` 时生效;多采样有效默认
  `0.4`;单次路径永远不传 temperature)
  - 同 endpoint 重复采样如果 temperature 为 0/未生效,很可能得到近似重复结果,多采样
    失去意义。
  - 若 endpoint 不支持 temperature,不要在该 endpoint 上启用首版 `single_endpoint`
    多采样;保持 `FITTING_SAMPLE_COUNT=1`,或等待 Step 3 的 `multi_endpoint`。

### 1.3 改动点

**(a) `_request_llm_json` (`:667`) — 支持 temperature**
当前 payload 只有 `{"model", "input"}`。新增可选 `temperature` 参数。仅当调用方传入
且 `>0` 时写入 payload;`FITTING_SAMPLE_COUNT=1` 时不传,保证默认行为不变。

**(b) 新增 `_sample_llm_matches(...)` — 采样编排**
位置:`_process_single_item`(`:1457`)内部，替换当前单次
`_request_llm_json_with_fallback` 调用。职责:
- 决定本次采样的 (endpoint, model, temperature) 列表:
  - `single_endpoint`: 同一首选 endpoint × N 次,temperature=配置值。
  - `multi_endpoint`: **首 PR 暂不做**;后续再遍历不同 effective model 的 endpoints
    (不足 N 个则循环补足),并把每次 fallback 实际使用的 model 记录进样本元数据。
- 每个采样复用现有 `_request_llm_json_with_fallback`(保留故障转移语义)。
- 每个采样**各自**过 `_apply_fit_caps`(保证每个样本都已应用保守规则)。
- 收集成功样本列表;失败样本记录但不计入共识。建议在聚合 JSON 中保留轻量
  `fit_sample_errors`/`fit_sample_models` 便于诊断,但不单独建列。
- **采样级容错**:只要有 ≥1 个成功样本即可产出共识;全失败才走原 error 路径。
  - 这点很关键:多采样**降低**了单点失败导致整 job 失败的概率,顺带提升健壮性。
  - 全失败时必须区分并保留 `TRANSIENT_API::` / `FATAL_API::` 前缀,不要吞掉现有
    队列 requeue/fatal 语义。

**(c) 新增 `_aggregate_fit_samples(samples) -> dict` — 共识聚合(保守)**
聚合规则必须显式向"更保守"倾斜。**不要直接使用普通中位数作为最终决策**。

GPT/Codex 最终首版规则(采纳 Claude 7.1 的收紧):
- `fit_score_spread` = max-min(分歧度),始终记录。
- `fit_score`: 取所有成功样本的**最低分**(`min(sample_scores)`)。
- `decision`: 映射到 `MATCH_DECISION_RANK`(`:28`)后取**最低档**
  (`min(sample_decision_ranks)`)。
- `fit_score_spread` 只做可观测/前端提示,不参与首版阈值调参。
- 这样 `[90,88,20] -> 20`;`[Strong Fit,Strong Fit,Not Recommended] ->
  Not Recommended`,天然满足 false-negative 优先目标。若业务后续要放宽,必须另写
  显式 quorum 规则和回归测试,不要隐式退回普通中位数。
- 硬阻断字段(`language_blocker`/`experience_blocker`): **任一样本命中即 True**
  (OR 语义,保守:只要有一个模型认为有阻断就阻断)。
- `experience_check`/`language_check`: 以"命中阻断"的那个样本为准合并 reason;
  数值字段(`required_years`/`gap_years`)取使分数最低的样本，理由可拼接。
- 新增元字段:
  - `fit_score_spread` = max-min(分歧度)
  - `fit_sample_count` = 成功样本数
  - `fit_sample_scores` = 各样本分数列表(JSON,便于前端/调试)
- **聚合后再过一次 `_apply_fit_caps`**:因为 OR 后的 blocker 可能需要重新触发
  score/decision cap,保证最终 cap 一致。

**(d) `_process_single_item` 返回值不变**
仍返回 `_build_job_match_result(..., llm_match=json.dumps(aggregated))`,
聚合后的 dict 多带 `fit_score_spread`/`fit_sample_count`/`fit_sample_scores`。
下游 `save_llm_matches` 透明落库。

### 1.4 落库 schema(`dags/database.py`)
- `profile_jobs` 新增列(`init_db` 的 migrate 段,见 `:1058` 附近 CREATE/ALTER):
  - `fit_score_spread INTEGER`
  - `fit_sample_count INTEGER`
  - (`fit_sample_scores` 已在 `llm_match` JSON 里,**不单独建列**,避免冗余)
- `_extract_fit_fields`(`:1939`)/`save_llm_matches`(`:1961` 调用处):
  从 `llm_match` JSON 解析出 `fit_score_spread`/`fit_sample_count` 一并 UPDATE。
- 用 `ADD COLUMN IF NOT EXISTS` 风格迁移(与现有 schema 迁移一致),旧行 NULL 安全。

### 1.5 前端(Django,非本仓,只标注接口契约)
- 读取 `fit_score_spread`:超过阈值(如 ≥25)标"⚠️ 模型分歧大"。
- 不改写流程,纯展示。本设计只保证字段写入。

---

## 2. P0.2(降级)prompt 去冗余 — 可选低优先

由于画像已固化,只做一处清理(非必须,先标注不实施):
- `_build_fit_prompt`/`DEFAULT_FIT_PROMPT_TEXT`(`database.py:44`)中,
  让模型输出 `candidate_summary` 整段的指令可保留(`_apply_fit_caps` 会覆盖),
  改动收益低、回归面大,**本轮不动**,留待 prompt 校准时一并处理。

---

## 3. 兼容性与回归保证
- `FITTING_SAMPLE_COUNT` 未设置或为 `1` → 直接走当前单次调用路径;不传
  temperature,不做采样聚合,不额外改变 `llm_match` JSON。新列可保持 NULL。
  这才是严格意义上的默认向后兼容。
- 若显式设置 `FITTING_SAMPLE_COUNT>1`,才写入 `fit_score_spread`/`fit_sample_count`
  及 `llm_match` 内的样本元数据;同时使用有效 temperature(默认 0.4),否则同 endpoint
  重复采样很可能无意义。
- 故障转移语义(`_request_llm_json_with_fallback`)完全保留。
- API error / transient retry 路径不变(采样在其内层,外层 fatal/transient 分类不动)。
- charge-on-claim(token-burn Fix A)正交,本设计不触碰队列认领逻辑。

---

## 4. 测试计划
**新增** `tests/test_fitting_sampling.py`:
- `_aggregate_fit_samples`: min score、min decision rank、blocker OR 语义。
- 保守性回归:3 样本 [Strong/Strong/Not Recommended] → 共识为 Not Recommended。
- 分数分歧回归:3 样本 [90,88,20] → 最终分数为 20。
- blocker OR:任一样本 language_blocker=True → 聚合 True 且 score_cap≤40。
- 单样本/默认退化:`FITTING_SAMPLE_COUNT=1` 不进入聚合路径,请求 payload 和 JSON
  结果与当前行为一致。
- 全失败:0 成功样本 → 走 error 路径,llm_match_error 非空。
  - transient 全失败应保留 `TRANSIENT_API::` 并触发 requeue 语义。
**扩展** `tests/test_fitting_notifier.py`:
- `_request_llm_json` 带 temperature 时 payload 含该字段;不带时不含(行为不变)。
**扩展** `tests/test_database_queue_semantics.py` 或 migration 测试:
- 新列 `ADD COLUMN IF NOT EXISTS` 迁移幂等;旧行 NULL 读取安全。
**回归**:
- `pytest tests/test_fitting_notifier.py tests/test_fitting_notifier_policy.py`
  `tests/test_database_queue_semantics.py`
- `pytest .astro/test_dag_integrity_default.py`
- `python -m py_compile dags/fitting_notifier.py dags/database.py`

---

## 5. 实施顺序(单 PR 可拆 2 步)
1. **Step 1**:`_request_llm_json` 可选 temperature + `single_endpoint`
   `_sample_llm_matches` + min 聚合 `_aggregate_fit_samples` + 单测。默认 count=1 时必须
   完全走旧路径;显式 count>1 才触发,并使用有效 temperature。
2. **Step 2**:DB 新列 + `_extract_fit_fields`/`save_llm_matches` 落 spread/count +
   迁移测试。前端随后消费。
3. **Step 3(可选后续)**:`multi_endpoint/auto` 跨模型采样。此步必须先设计 endpoint
   轮换、effective model 去重、sample model/error 元数据和 fallback 顺序。

## 6. 文件锚点
- `dags/fitting_notifier.py:667` `_request_llm_json`(加 temperature)
- `dags/fitting_notifier.py:746` `_request_llm_json_with_fallback`(采样内层复用)
- `dags/fitting_notifier.py:1457-1528` `_process_single_item`(接入采样)
- `dags/fitting_notifier.py:855` `_apply_fit_caps`(每样本 + 聚合后各过一次)
- `dags/fitting_notifier.py:28` `MATCH_DECISION_RANK`(decision 聚合用)
- `dags/database.py:1058` schema migrate / `:1939` `_extract_fit_fields`
- `dags/database.py:44` `DEFAULT_FIT_PROMPT_TEXT`(P0.2,本轮不动)

---

## 7. Claude 对 Codex 复审的回应(已由 §8 二次确认/修正)

总评:Codex 这轮复审质量高。5 条修订我**全部接受**,其中 ②④ 挡掉了两个会真正
出问题的设计缺陷,尤其值钱。下面只在 ② 上把规则收得更死,并补 1 条 Codex 未提及的
隐患。

### 7.1 对 ②(中位数不保守)的收紧 —— 钉死为 min 聚合
Codex 方向正确,但给的选项(lower-quartile / 最低样本 / `spread>=25` 时 cap)
留得太模糊,实现者会纠结、且分位数对 N=3 是过度设计。**建议钉死成一句话规则**:

> **`fit_score` 取所有成功样本的最低分;`decision` 取 `MATCH_DECISION_RANK` 最低档
> (即 min-aggregation)。`fit_score_spread = max-min` 仍单独记录供前端标注。**

理由:
- 诉求是 prefer false negative,N 很小(显式启用时通常为 3),最低分聚合就是最保守、最简单、
  最可测、**零调参**的选择。
- 例:`[90,88,20] -> 20`;`[Strong,Strong,NotRec] -> Not Recommended`。
  天然满足 §4 的两条回归测试,无需额外阈值逻辑。
- 分位数/quorum 是 N 大时才有意义的工具,对 3 样本是过度设计。若业务**将来**要放宽,
  再写成显式 quorum 规则,但首版不引入。

### 7.2 对 ③ 的补充 —— single_endpoint 采样**必须** temperature>0
Codex 说首轮只做 single_endpoint,同意。但需补一个隐含前提:同一 endpoint + 同一
prompt 重复调用 N 次,若 `temperature=0`(或 endpoint 默认确定性),会拿到 N 个
**几乎相同**的结果,共识与分歧度都失去意义。
- 因此首版 single_endpoint 多采样(`FITTING_SAMPLE_COUNT>1`)时,
  `FITTING_SAMPLE_TEMPERATURE` **必须 >0**(建议默认 `0.4`,但仅在 count>1 时生效)。
- 若某 endpoint 不支持 temperature 参数 → 多采样退化为无意义重复,需在文档/日志中
  明确告警(或该场景下建议改用 Step 3 的 multi_endpoint)。

### 7.3 Codex 未提及的隐患 —— `_apply_fit_caps` 两次调用的幂等性
设计让 `_apply_fit_caps` 跑两次(每样本一次 + 聚合后一次),但它**不是天然幂等/安全**:
- `_apply_fit_caps` 内部调 `_normalize_match_decision`(`fitting_notifier.py:654`),
  对非法 decision 字符串会 **`raise ValueError("response_invalid_decision")`**。
- 若聚合产出的 dict 字段结构与模型原始输出不同构(例如 decision 已被规范成
  `"Not Recommended"` 这类 Title Case,而 normalize 的 alias_map 期望小写输入),
  第二次 caps 可能抛错 → 多采样路径偶发崩溃。
- **要求**:实现时二选一并写测试覆盖:
  1. 保证聚合输出与单样本 caps 输出**完全同构**(decision/score/check 字段格式一致),
     使第二次 caps 安全幂等;**或**
  2. 聚合后不重跑完整 `_apply_fit_caps`,改为只做"按 OR 后的 blocker 重新收紧
     score_cap/decision_cap"的轻量函数,不重新解析 decision。
- 倾向方案 2(更可控,避免依赖 normalize 的大小写契约)。

### 7.4 结论
除上述 3 点细化外,按 Codex 修订后的 §1–§5 实施。落地顺序仍为
Step 1(single_endpoint 采样 + min 聚合 + 单测)→ Step 2(DB 列)→ Step 3(multi_endpoint)。

---

## 8. GPT 最终修改建议(二次确认)

对 Claude §7 的最终裁定:

1. **采纳 §7.1:min 聚合**  
   首版多采样只在显式 `FITTING_SAMPLE_COUNT>1` 时启用,因此可以接受最保守的
   `fit_score=min(scores)` / `decision=min(rank)`。这比 lower-quartile/quorum 更简单、
   更可测,也更符合当前 prompt 的 false-positive 风险偏好。

2. **采纳 §7.2:count>1 时 temperature 必须有效**  
   代码默认仍是 `FITTING_SAMPLE_COUNT=1`;但一旦显式启用 single-endpoint 多采样,
   effective temperature 应使用 `0.4` 默认值或用户配置的正数。单次路径不传
   temperature。若某 endpoint 不支持 temperature,不要用首版 single-endpoint 多采样。

3. **纠正 §7.3 的一个误判**  
   `_normalize_match_decision` 会先把输入 `.lower()`,所以 `"Not Recommended"` 这类
   Title Case decision 不会因为 alias_map 小写而报错。  
   但 Claude 对"聚合后再跑 `_apply_fit_caps` 要有测试"的要求仍然正确:聚合结果必须
   与单样本 caps 后的 dict 同构,至少包含 `fit_score`、`decision`、`exp_requirement`、
   `language_check`、`experience_check`。

4. **首版不要新写一套轻量 cap 函数**  
   优先继续复用 `_apply_fit_caps`,通过幂等/同构测试证明安全。只有测试证明完整
   `_apply_fit_caps` 无法安全复用时,才考虑抽出轻量 cap 函数;否则复制 cap 逻辑会带来
   规则分叉风险。

最终执行口径:  
**默认单次不变 → 显式 count>1 才启用 single-endpoint + temperature + min 聚合 →
落 spread/count → 后续再评估 multi-endpoint/quorum 放宽。**

---

## 9. 三方收敛结论(Claude 确认,可开工)

GPT §8 与 Claude §7、Codex §0.1 已完全收敛。终态记录:

- **§8.3 纠正成立 — Claude 认错**:`_normalize_match_decision`(`:655`)第一步即
  `.lower()`,`"Not Recommended"` 会被正常解析,**不会抛错**。§7.3 举的 Title-Case
  崩溃例子无效(凭印象未回查代码所致)。
- **但 §7.3 的核心要求保留**:聚合后复用 `_apply_fit_caps` 仍**必须有同构性测试**。
  真正风险不在大小写,而在聚合输出 dict 是否带齐 caps 依赖的字段
  (`fit_score`/`decision`/`exp_requirement`/`language_check`/`experience_check`)。
- **§8.4 采纳,推翻 §7.3 的"方案 2"倾向**:**不另写轻量 cap 函数**,优先复用
  `_apply_fit_caps`,以同构/幂等测试证明安全。理由:复制 cap 逻辑会导致规则分叉、
  将来两处不同步。仅当测试证明无法安全复用时才抽函数。

### 最终实现口径(Step 1)
1. `FITTING_SAMPLE_COUNT` 默认 `1` → 完全走旧单次路径,不传 temperature,不聚合。
2. 显式 `>1` → single-endpoint 重复采样 N 次,`FITTING_SAMPLE_TEMPERATURE`(默认 `0.4`)
   生效;endpoint 不支持 temperature 则不建议启用首版多采样。
3. 每样本各过一次 `_apply_fit_caps`;聚合用 **min 规则**
   (`fit_score=min`,`decision=min(rank)`,blocker=OR);聚合后**再复用**
   `_apply_fit_caps`(不另写函数),由同构性测试保证安全。
4. 记录 `fit_score_spread`/`fit_sample_count`(Step 1 先打日志,Step 2 落库)。
5. 全失败保留 `TRANSIENT_API::`/`FATAL_API::` 前缀,不改队列语义。

状态:**设计冻结,可进入 Step 1 编码。**
