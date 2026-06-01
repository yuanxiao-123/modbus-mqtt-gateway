# Modbus-to-MQTT Gateway

基于 C 语言实现的工业级 Modbus RTU 转 MQTT 网关，支持多设备采集、离线消息持久化与断线自动重连。

## 功能特性

- 通过串口（RS-485）轮询多台 Modbus RTU 从机，读取温度和湿度寄存器
- 将数据以 JSON 格式发布到 MQTT Broker
- MQTT 断线时消息自动缓存至 SQLite，重连后补发，不丢数据
- 支持最多 8 台设备，每台独立配置从机地址、寄存器、MQTT 主题
- 多线程架构：采集线程与发布线程解耦，互不阻塞
- 串口/MQTT 断线均自动重连（含指数退避）
- 支持 MQTT LWT（遗嘱消息），网关异常离线时 Broker 自动发布 `offline`
- 每 60 轮输出各设备读取成功/失败统计
- 注册 `SIGINT`/`SIGTERM`，支持 Ctrl-C 和 systemd stop 优雅退出
- 退出时将内存队列中未发送的消息落地 SQLite，防止丢失

## 目录结构

```
gateway-project/
├── gateway.c        # 网关主程序
├── modbus_slave.c   # Modbus 从机模拟器（测试用）
├── gateway.conf     # 配置文件
└── Makefile         # 构建脚本
```

## 依赖

| 库 | 用途 |
|----|------|
| libmodbus | Modbus RTU 通信 |
| libpaho-mqtt3c | MQTT 客户端 |
| libsqlite3 | 消息持久化 |
| libpthread | 多线程 |

在 Debian/Ubuntu 上安装：

```bash
sudo apt install libmodbus-dev libpaho-mqtt-c-dev libsqlite3-dev
```

## 编译

```bash
make          # 编译 gateway 和 modbus_slave
make clean    # 清理二进制和数据库文件
```

## 配置文件

编辑 `gateway.conf`：

```ini
[serial]
port=/dev/ttyUSB0      # 串口设备
baudrate=9600          # 波特率
poll_interval=1        # 轮询间隔（秒）

[mqtt]
broker=tcp://localhost:1883
client_id=modbus_mqtt_gateway
qos=1                  # 0/1/2

[database]
path=gateway.db        # SQLite 数据库路径

[device_01]
slave_id=1             # Modbus 从机地址
reg_temp=0             # 温度寄存器地址
reg_humidity=1         # 湿度寄存器地址
topic=factory/sensor/01  # MQTT 发布主题
```

支持最多 8 个 `[device_XX]` 节，按 `device_01`、`device_02`…… 命名。

## 运行

### 使用真实硬件

```bash
./gateway
```

### 使用模拟器测试（无硬件）

先创建虚拟串口对（需要 `socat`）：

```bash
socat -d -d pty,raw,echo=0 pty,raw,echo=0
# 输出示例：/dev/pts/2 <-> /dev/pts/3
```

在终端 1 启动从机模拟器（监听 /dev/pts/3）：

```bash
./modbus_slave
```

修改 `gateway.conf` 中 `port=/dev/pts/2`，在终端 2 启动网关：

```bash
./gateway
```

## MQTT 消息格式

**传感器数据**（发布到 `topic` 配置的主题）：

```json
{"ts":1717200000,"temp":25.6,"humidity":60.2}
```

| 字段 | 说明 |
|------|------|
| `ts` | Unix 时间戳（秒） |
| `temp` | 温度，寄存器原始值 ÷ 10 |
| `humidity` | 湿度，寄存器原始值 ÷ 10 |

**网关状态**（主题 `gateway/status`，retained）：

| 值 | 触发时机 |
|----|---------|
| `online` | 网关启动或重连成功 |
| `offline` | 网关异常断线（由 Broker LWT 发布） |

## 架构说明

```
串口 (RS-485)
    │
    ▼
collect_thread          publish_thread
─────────────           ──────────────
modbus_read_registers   queue_pop
    │                       │
    ▼                       ▼
queue_push ──────────► MQTTClient_publishMessage
                            │
                        失败时写入 SQLite
                        重连后 db_replay 补发
```

- **SafeQueue**：容量 32，环形缓冲区，mutex + condvar 保证线程安全；队列满时带超时等待，避免采集线程在 MQTT 长时间断线时永久阻塞。
- **db_replay**：重连成功后按插入顺序补发缓存消息，补发成功后删除记录，防止数据库无限增长。

## 日志格式

```
[2024-06-01 12:00:00][INFO ] [Modbus] 设备1: {"ts":1717200000,"temp":25.6,"humidity":60.2}
[2024-06-01 12:00:05][WARN ] [MQTT] 连接失败，1秒后重试...
[2024-06-01 12:00:06][ERROR] [DB] 打开数据库失败: unable to open database file
```

级别：`INFO`（正常）、`WARN`（可恢复异常）、`ERROR`（严重错误）。

## 停止网关

```bash
Ctrl-C
# 或
kill -SIGTERM <pid>
```

收到信号后，网关会等待当前轮次采集完成，将内存队列中的消息持久化到 SQLite，然后退出。

