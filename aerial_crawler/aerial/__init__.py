"""地方测绘档案馆航拍底片采集项目。

包结构：
- aerial.config    运行配置
- aerial.limiter   全站共享的最小发起间隔限流器 / 统一暂停
- aerial.http      带退避重试的 HTTP 客户端
- aerial.parsing   分页目录与卷宗详情 HTML 解析
- aerial.state     工作目录状态（扫描水位、失败待补、目录锁）
- aerial.manifest  JSON Lines 清单与“当前条目”原子视图
- aerial.downloader 分段影像下载、断点区段复用、校验装配
- aerial.scanner   全量 / 增量采集调度
"""

__version__ = "1.0.0"
