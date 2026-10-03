# 问答 Agent（M7）

已实现独立问答任务、受控 Responses 循环、七个知识工具、原文摘录验证和待确认修改；本地验证范围见[测试说明](../../docs/testing.md)。配置与恢复见[微信知识库问答](../../docs/agent-setup.md)。

| 文件 | 职责 |
| --- | --- |
| `models.py`、`repository.py` | 问答/调度/确认账本、租约、重试、原子保存答案与 outbox |
| `queue.py`、`worker.py` | 独立 Redis Stream 的可靠发布、领取和消费 |
| `engine.py` | 单问题、默认最多六轮的 Responses 工具循环，不创建远端会话 |
| `tools.py` | 严格参数的七个知识工具；租户在服务端绑定，来源引用仅本轮有效 |
| `security.py` | active/allowlist 与来源快照共享锁、本地证据内容校验 |
| `grounding.py` | 最多三段、每段 400 字符的逐字摘录验证，后端引用渲染及微信文本拆分 |
| `contracts.py` | 问答任务、结果、操作提案和通知 Protocol |

外部调用位于 `app/adapters/openai_responses.py` 和既有知识 adapter；微信 admission、通知和发送保护位于 `app/connectors/wecom/agent.py`。进程资源装配与 `--retry <dispatch-uuid>` 运维入口位于 `app/workers/agent.py`。

工具不接受 `user_id`、`vector_store_id` 或任意来源 UUID；删除和标签工具只生成提案。真实用户在同一会话内回复十分钟有效的确认码后，后端重新鉴权并在事务内执行。资料内容、模型输出或引用均不是修改授权。

模型只能选取已验证证据中的原句，后端添加来源标题、类型、原 URL、收录日期与可验证的片段定位；没有依据时返回“没有找到相关资料。”。当前不实现自由改写的综合回答或跨问题聊天记忆，也不声明已完成真实 OpenAI/企业微信公网联调。
