# 开发说明

项目使用 Python 标准库提供本地 HTTP 服务，并通过中文网页展示会话搜索、历史检查、备份清理与回滚功能。运行时不调用模型，不上传聊天数据。

## 实现结构

- `app.py`：HTTP 接口、静态资源、本地请求校验和服务生命周期。
- `session_catalog.py`：会话搜索、元数据读取与子代理关系查询。
- `repair_service.py`：检查任务、批量清理、备份、回滚与中断恢复。
- `repair_encrypted_session.py` 和 `repair_split_encrypted_session.py`：独立会话与分段历史的修复引擎。
- `web/`：页面、交互脚本与样式。

清理前保存文件与索引快照；清理后出现新消息时，阻止覆盖式回滚。加密内容本身不代表损坏，检查与清理条件见 README。

## 开发验证

自动测试使用合成会话和独立数据目录，覆盖历史清理、子代理批量处理、回滚、中断恢复、文件占用、过期快照与 HTTP 请求保护。

`tests/run_demo.py` 提供合成演示数据；`tests/ui_smoke.py` 使用 Playwright 检查页面操作、备份导出和窄屏布局。测试产物保存在已被 Git 忽略的 `test-artifacts/` 中。
