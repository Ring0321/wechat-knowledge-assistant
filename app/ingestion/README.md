# 统一收录编排（M3）

统一契约位于 `app/domain/documents.py`。M3 建立 detection、repository、queue、worker 和 pipeline，M4/M5 扩展解析，M6 在独立持久任务中索引，成功后产生 ready。Canonical JSON 与 Markdown 及原件通过 ObjectStore 写入私有存储。M8 卡片保持 metadata_only；channel_supplement 任务在原来源新增用户补充的视频和派生内容，失败保留原卡片，有正文才接续索引。

任务由可信微信 admission 在同一数据库事务写入，Redis 只传 dispatch UUID。业务通过 MediaDownloader/ObjectStore Protocol 调用外部 adapter。详见根目录 architecture.md 和 docs/ingestion-setup.md。
