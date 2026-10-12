#!/bin/sh
# 方案 B 回归验证脚本
# ────────────────────────────────────────────────────────────────
# 用途：验证 0.0.4+ 版本在 Aliyun ARMS Java agent（或其他 OTel javaagent）
#       存在的 JVM 中，trace 父子关系依然完整、无需任何 workaround flag。
#
# 前置：
#   1. 下载阿里 javaagent 到 .testbench/aliyun-java-agent.jar：
#        mkdir -p .testbench && \
#        curl -L -o .testbench/aliyun-java-agent.jar \
#          http://arms-apm-cn-shanghai.oss-cn-shanghai.aliyuncs.com/aliyun-java-agent.jar
#   2. 本地安装 SDK: mvn clean install -DskipTests -Dgpg.skip=true
#   3. 构建 demo:    mvn -f demo/pom.xml clean package -DskipTests
#   4. 配置 .env 的 CLS_* 凭证
#
# 预期：日志里 "GlobalOpenTelemetry = ApplicationOpenTelemetry127"（阿里 agent 已占全局），
#       业务链路跑完，无 OTLP 错误；CLS 侧按 sessionId 可查到完整 7-span 单 trace。
# ────────────────────────────────────────────────────────────────
set -e
cd "$(dirname "$0")/.."
exec java \
  -javaagent:.testbench/aliyun-java-agent.jar \
  -jar demo/target/demo-shaded.jar
