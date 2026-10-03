# 企业微信入口边界（M2）

已实现官方微信客服 callback、crypto、sync_msg/send_msg adapter、Redis token cache、通知队列、归一化、游标/回复 outbox、同步与回复 worker 逻辑。业务层依赖 contracts.py Protocol / DTO。

媒体只使用实际返回的 media_id；M3 官方 media adapter 负责下载。M8 独立卡片 parser 位于 app/parsers/channels.py，后补命令桥接位于本目录 channels.py，调用既有异步收录。启用方法见根目录 docs/wecom-setup.md 和 docs/channels-setup.md；默认关闭，无凭证不连接真实企业微信。
