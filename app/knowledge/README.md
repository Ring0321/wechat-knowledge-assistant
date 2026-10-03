# 知识服务（M6）

`KnowledgeService` 封装 index_document / delete_document / search / get_source / list_sources；内部认证租户绑定独立 Vector Store，使用官方 File Search 的 Vector Store search 检索层。`KnowledgeProcessor` 在现有 worker 执行可恢复上传、索引轮询和删除；`jobs` 保证与正文/tombstone 同事务入队；`backfill` 只处理已保存正文。

见[配置及使用](../../docs/knowledge-setup.md)和[测试说明](../../docs/testing.md)。Responses Agent 通过受控工具调用本服务，配置见[微信问答](../../docs/agent-setup.md)。
