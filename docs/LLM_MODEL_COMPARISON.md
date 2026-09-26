# JD 模型比较流程

用户说「比较这些模型」时，直接从远程 DB 读取 JD，用指定模型完成真实 fitting，保存分数和原因，再根据结果做对比。交付物是实际结果和分析，不能只交付脚本、计划或测试通过的说明。

下次直接说：

> 按 docs/LLM_MODEL_COMPARISON.md，从远程 DB 取最新 50 条 JD，用模型 A、B、C 跑 fitting 并比较。

## 必须遵守的执行方式

- **不编写或运行任何测试**：不跑 pytest、mock、smoke test、DAG integrity 或 Airflow test mode，不额外发起一轮试跑。
- **不使用 Docker**：包括一次性容器、已有 Airflow 镜像和 Docker Compose。
- **不启动 Astro/Airflow 或整个项目**：不启动 web、scheduler、worker，不触发 DAG。
- 使用本地 Python 进程直接连接远程 DB 和模型 endpoint。缺依赖就在本地虚拟环境补齐，不转用容器。
- DB 连接设为只读，SQL 仅使用 SELECT；不初始化 schema、不更新队列、不写 fitting 结果、不发 Discord 通知。
- JD 来自 DB 已保存的描述，不扫描 LinkedIn、不访问网页抓取 JD。
- 不调用 Claude CLI；模型以用户当次要求为准，不擅自更换模型或加入额外评审模型。
- 本流程不适用 AGENTS.md 中的 Real Airflow Test Mode 要求。

配置读取、样本数量确认、结果文件计数属于执行步骤，不扩展成测试工程。

## 1. 读取配置和固定样本

默认 profile 为 Xingyou Li、样本 50 条、并发 3；用户指定的参数优先。

通过现有 load_env() 读取环境配置，包括根目录 .env 和 dags/.env。使用 JOBS_DB_URL 直接连接远程 Postgres，不输出密钥或完整配置值。

取样规则：

1. 查找启用的 profile，读取其生产 prompt、简历、candidate summary 和近期反馈，作为本次共同输入。
2. 从 profile_jobs 关联 jobs，仅取该 profile 关联且 description 非空的 JD。
3. 按 batches.timestamp DESC NULLS LAST、jobs.batch_id DESC NULLS LAST、profile_jobs.last_seen_at DESC NULLS LAST、jobs.id DESC 排序。
4. 取指定数量，不足时明确报告，不默默缩小样本或重新抓取网页。
5. 所有模型共享同一批 job ID 和输入，不能为每个模型重新拉取一批最新 JD。

「最新」指 DB 中的批次与关联时间顺序，不等于 LinkedIn 原始发布日期。

## 2. 用现有 prompt 跑指定模型

调用生产 _build_fit_prompt()，使用 profile 的 fit_prompt_config；缺省配置沿用生产函数的默认模板。每条 JD 只构建一次 prompt，再原样发给全部模型。**不另写 fitting prompt，不为不同模型修改候选人基准或其他输入。**

当前脚本的模型配置：

| 模型前缀 | 配置来源 | 调用方式 |
| --- | --- | --- |
| deepseek-* | Deepseek_API_ENDPOINT、Deepseek_API_KEY | Chat Completions |
| grok-* | Grok_API_ENDPOINT、Grok_API_KEY | Chat Completions |
| gpt-* | LLM_ENDPOINTS_JSON | 本次比较使用配置好的 Responses endpoint |

DeepSeek/Grok 的基础 URL 会由比较脚本补全为 /v1/chat/completions。生产配置中的 request_url 则需要完整 HTTP 请求地址。

GPT 分支直接读取 LLM_ENDPOINTS_JSON。如果生产已切换到 Grok，给本地比较进程提供正确的 GPT Responses 配置，不要修改服务器生产设置。确认 endpoint 的 model 覆盖项没有替换用户指定的模型。

当前脚本只识别上述三个前缀。用户指定其他供应商时，仅做必要的本地调用适配，不扩展为整个项目的重构或测试任务。

每个成功响应统一执行生产 _validate_llm_match_response() 和 _apply_fit_caps()。记录 caps 后的最终 fit_score，decision 仅用于生产响应校验。

## 3. 本地执行并等到完成

在仓库根目录，使用具备依赖的本地 Python 环境：

```bash
python scripts/compare_llm_models.py \
  --count 50 \
  --profile "Xingyou Li" \
  --models deepseek-flash gpt-6-sol grok-4.7 \
  --concurrency 3
```

模型列表只是示例，执行时替换为用户当次指定的模型。python 应指向本地虚拟环境，也可直接使用其解释器的绝对路径。使用 --output-dir 指定独立产物目录，保留既有结果。

当前脚本导入 dags.fitting_notifier，除了 psycopg、pandas、requests、python-dotenv，还需要提供 airflow.sdk 的兼容 Python 包；根目录 requirements.txt 未声明完整的 Airflow 导入依赖。**依赖 Python 包不意味着要启动 Airflow 服务。** 本地缺依赖时补齐所需包，不能回退到 Docker。之前在容器中的成功运行也不能当作本地环境已经就绪。

目标结果数为 JD 数量 × 模型数量。例如 50 × 3 应有 150 个模型槽位；必须分别统计成功和失败，不能把 150 个槽位说成 150 次成功。

- 等待真实调用结束、文件生成，不能只启动进程就宣布完成。
- 瞬时错误可有限重试，fallback 可能使实际 HTTP 请求数超过逻辑结果数。
- 失败保留明确 error，不用 0 分冒充失败结果。
- 只补跑失败的 job_id + model，保留其他成功结果和原始失败记录，另存更新版。
- 补跑应保持原始 JD 和 prompt 输入；重新读 DB 后无法确认输入一致时，要说明限制。
- 认证、模型不可用等持续错误应如实报告，不无限重试或擅自换模型。

现有脚本整批结束后写文件，没有内置断点续跑或 --retry-failed 参数。不要编造参数；必要时针对失败项调用现有函数补跑。

## 4. 保存分数、原因和调用状态

输出 JSON、CSV、Markdown：

- JSON 保存完整结构化记录。
- CSV 每个 JD 一行，按模型展开 score、reason、status、error。
- Markdown 按 JD 展示各模型分数、原因及错误，供阅读。

```json
{
  "job_id": "...",
  "title": "...",
  "company": "...",
  "models": {
    "requested-model": {
      "fit_score": 72,
      "reason": {
        "summary": "...",
        "experience": "...",
        "language": "...",
        "risks": [],
        "missing_critical_skills": []
      },
      "status": "success",
      "error": null,
      "response_model": "...",
      "attempts": 1,
      "latency_seconds": 2.31
    }
  }
}
```

失败使用 status: error，fit_score 和 reason 为 null，并记录 error。attempts 是脚本评测循环次数，不一定等于 HTTP 请求数，因为单轮可能包含多个 fallback endpoint。

产物不保存 API key、完整 prompt、简历、反馈原文或完整 JD。原因中的缺失字段如实说明，不能把字段存在当成内容完整。

## 5. 读取结果并比较

默认完成 fitting 后，由当前助手读取产物做对比；用户说「只记录」时，只交付数据，留给用户指定的第三方模型分析。未经用户要求，不额外调用付费评审模型。

比较重点：

1. 每个模型的成功/失败数量，以及是否覆盖同一组 job ID。
2. 分数分布、主要分歧和共同判断；与 GPT 接近不等于正确。
3. 重点看分差较大的 JD 和边界岗位，评价岗位方向、语言、经验、申请资格，以及必备技能与可培训技能的区分。
4. 使用具体岗位例子支撑结论。用户要求模型评分时，说明评分维度和主观性质，不把它当作人工标注准确率。
5. 区分模型理由与 caps 后处理追加内容；说明候选人基准不一致、字段缺失、重复岗位或低分样本过多等实际限制。
6. 响应时间可作辅助信息；未记录真实费用时，不声称成本排名。

脚本负责记录，当前助手或用户指定的第三方负责分析，不需要为了总结再搭建评测框架。

## 完成时交付

- 实际 JD 数量、指定模型列表、各模型成功/失败数量。
- JSON、CSV、Markdown 文件链接，以及补跑记录或未解决错误。
- 用户要求的比较、例子和评分；若要求只记录，则交付可供第三方分析的数据。

如果只是修改脚本或文档，还没有调用模型，必须明确说「尚未执行 fitting」。
