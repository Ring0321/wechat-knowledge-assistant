# 内容解析（M4/M5/M8）

M4 已实现 PDF 文本/扫描 OCR、DOCX、XLSX、PPTX、图片 OCR、WebPageParser 和 WeChatArticleParser。通过 LocalFileParser 的受限子进程运行，结果统一为 ParsedContent，再由 ingestion 转 CanonicalDocument。能力和限制见根目录 architecture.md 与 docs/parsing-setup.md。

M5 已加入 AudioVideoParser，通过领域接口编排 ffmpeg 提取、OpenAI 转录和画面描述，并合并真实时间轴；默认关闭，本地真实服务集成验收已通过，OpenAI使用HTTP模拟。M8 WeChatChannelsParser 只解析官方卡片白名单字段，正文保持为空；原视频由用户明确补充后复用 AudioVideoParser，见根目录 docs/channels-setup.md。
