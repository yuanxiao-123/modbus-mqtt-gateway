# Modbus RTU → MQTT 工业协议网关

基于 Linux C 实现的工业协议网关，将 Modbus RTU 传感器数据采集后通过 MQTT 协议上报至云平台。

## 功能特性

- Modbus RTU 多寄存器轮询采集（基于 libmodbus）
- MQTT QoS1 可靠发布（基于 paho-mqtt-c）
- 断网时数据自动缓存至 SQLite，重连后按序补发
- 多线程架构：采集线程与发布线程解耦，互不阻塞
- 串口/网络异常自动重连

## 系统架构

[Modbus从站] ──RS485──► [采集线程] ──队列──► [发布线程] ──TCP──► [MQTT Broker]
│ 断线
[SQLite缓存]
│ 重连
[补发回放]



## 依赖

```bash
sudo apt install libmodbus-dev libpaho-mqtt-dev libsqlite3-dev
编译

make
运行

# 启动 Modbus 从站模拟（开发测试用）
socat -d -d pty,raw,echo=0 pty,raw,echo=0   # 创建虚拟串口对
./modbus_slave                               # 启动模拟从站

# 启动网关
./gateway
配置
修改 gateway.c 顶部的宏定义：

参数	默认值	说明
SERIAL_PORT	/dev/pts/2	串口设备路径
SLAVE_ID	1	Modbus从站地址
POLL_INTERVAL	1	采集间隔（秒）
BROKER_URL	tcp://localhost:1883	MQTT Broker地址
TOPIC	factory/sensor/01	发布Topic
数据格式

{
  "temp": 25.6,
  "humidity": 60.2
}
