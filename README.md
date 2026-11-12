# Hangar Ingest 维修遥测接收

这是一个可独立运行的本地业务项目。核心模块保留数据接收、领域规则、持久化记录及查询接口；演练数据均为合成资料，不需要真实账号或外部业务服务。

## 运行环境

Python 3.11 或更新版本，核心业务使用标准库与 SQLite。

Dockerfile 固定运行时版本。首次准备执行 docker build -t local-product .，之后以 docker run --rm --network none local-product 运行完整检查；所有 HTTP 演练只在容器内部回环地址通信。

## 开发与运行

业务测试：
    python -m unittest discover -s tests -v
编译或语法检查：
    python -m compileall -q hangar_ingest demo_support tests
现有业务演示：
    python -m hangar_ingest --db :memory: demo --fixture fixtures/demo.json
输入端演练检查：
    python demo_support/check.py
目录采集器端到端演练（轮转、半条、迟到尾部、崩溃重启）：
    python demo_support/collector_drill.py
合并运行：
    sh demo_support/verify.sh

## 目录采集器

采集器长期跟随一个传感 CSV 目录，也能按预算只扫描一轮（`collect ... --budget-bytes`，
预算按文件分配，高频日志不会长期挤占其他文件）。

- 文件身份基于 inode 而非路径：改名后旧文件迟到的尾部仍归同一身份；同名替换与
  同 inode 截断重写都会开启新世代，旧世代以 `replaced` / `truncated` 状态保留。
- 字节级分帧只在完整 CSV 记录处确认进度；带引号字段可跨行，多字节 UTF-8（如中文）
  可分两次写入，EOF 处的半条内容原样留到下次扫描。
- 完整记录复用既有解码、校准与批次入库；每条结果都带原始文件世代与字节范围
  （`collector_lineage`），可从读数反查原始字节。
- 业务入库与文件检查点在同一数据库事务内提交：导入成功后、检查点未保存便崩溃时，
  事务整体回滚，重启不会产生第二份有效读数。

命令行：
    python -m hangar_ingest --db hangar.sqlite3 collect <日志目录> --source feed1 --name hangar
    python -m hangar_ingest --db hangar.sqlite3 collect <日志目录> --source feed1 --name hangar --positions


输入协议、样本与故障开关见 demo_support/协议.md。输入端检查只证明本地来源的约定行为；业务演示展示当前已有功能。调用者可以在独立临时数据目录重复运行，原始样本不会被覆盖。
