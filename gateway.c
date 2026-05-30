#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#include <modbus/modbus.h>
#include <MQTTClient.h>
#include <sqlite3.h>
#include <ctype.h>
#include <stdarg.h>
#include <time.h>
#include <stdint.h>

/* ─────────────── 运行标志 ─────────────── */
static volatile sig_atomic_t g_running = 1;

static void sig_handler(int sig) {
    (void)sig;
    g_running = 0;
}

/* ─────────────── 日志 ─────────────── */
typedef enum { LOG_INFO, LOG_WARN, LOG_ERROR } LogLevel;

void log_write(LogLevel level, const char *fmt, ...) {
    struct tm tm_buf;
    time_t now = time(NULL);
    char timebuf[32];
    strftime(timebuf, sizeof(timebuf), "%Y-%m-%d %H:%M:%S",
             localtime_r(&now, &tm_buf));   /* localtime_r 线程安全 */

    const char *ls = (level == LOG_WARN)  ? "WARN " :
                     (level == LOG_ERROR) ? "ERROR" : "INFO ";

    /* 拼入单个 buf 后一次 fputs，多线程下日志行不会交叉 */
    char buf[512];
    int n = snprintf(buf, sizeof(buf), "[%s][%s] ", timebuf, ls);
    va_list args;
    va_start(args, fmt);
    vsnprintf(buf + n, sizeof(buf) - (size_t)n, fmt, args);
    va_end(args);
    size_t len = strlen(buf);
    if (len < sizeof(buf) - 1) { buf[len] = '\n'; buf[len + 1] = '\0'; }
    fputs(buf, stdout);
    fflush(stdout);
}

/* ─────────────── 设备列表 ─────────────── */
#define DEVICE_MAX 8

typedef struct {
    int      slave_id;
    int      reg_temp;
    int      reg_humidity;
    char     topic[128];
    uint32_t ok_count;    /* 读取成功次数 */
    uint32_t fail_count;  /* 读取失败次数 */
} DeviceConfig;

/* ─────────────── 配置 ─────────────── */
typedef struct {
    char serial_port[64];
    int  baudrate;
    int  poll_interval;
    char broker[128];
    char client_id[64];
    int  qos;
    char db_path[64];
    DeviceConfig devices[DEVICE_MAX];
    int          device_count;
} Config;

Config g_cfg;

static char *trim(char *s) {
    while (isspace((unsigned char)*s)) s++;
    char *e = s + strlen(s) - 1;
    while (e > s && isspace((unsigned char)*e)) *e-- = '\0';
    return s;
}

void config_load(const char *path) {
    strncpy(g_cfg.serial_port, "/dev/pts/2",           63);
    strncpy(g_cfg.broker,      "tcp://localhost:1883", 127);
    strncpy(g_cfg.client_id,   "modbus_mqtt_gateway",   63);
    strncpy(g_cfg.db_path,     "gateway.db",            63);
    g_cfg.baudrate      = 9600;
    g_cfg.poll_interval = 1;
    g_cfg.qos           = 1;
    g_cfg.device_count  = 0;

    FILE *f = fopen(path, "r");
    if (!f) {
        log_write(LOG_WARN, "[CFG] 配置文件不存在，使用默认值");
        return;
    }

    char line[256], section[64] = "";
    /* 修复：去掉 static，避免函数重入时残留上次状态 */
    char last_section[64] = "";
    int  last_idx = -1;

    while (fgets(line, sizeof(line), f)) {
        char *p = trim(line);
        if (*p == '#' || *p == '\0') continue;

        if (*p == '[') {
            char *e = strchr(p, ']');
            if (e) { *e = '\0'; strncpy(section, p + 1, 63); }
            continue;
        }

        char *eq = strchr(p, '=');
        if (!eq) continue;
        *eq = '\0';
        char *key = trim(p);
        char *val = trim(eq + 1);

        if (strcmp(section, "serial") == 0) {
            if      (strcmp(key, "port")         == 0) strncpy(g_cfg.serial_port, val, 63);
            else if (strcmp(key, "baudrate")      == 0) g_cfg.baudrate      = atoi(val);
            else if (strcmp(key, "poll_interval") == 0) g_cfg.poll_interval = atoi(val);
        } else if (strcmp(section, "mqtt") == 0) {
            if      (strcmp(key, "broker")    == 0) strncpy(g_cfg.broker,    val, 127);
            else if (strcmp(key, "client_id") == 0) strncpy(g_cfg.client_id, val,  63);
            else if (strcmp(key, "qos")       == 0) g_cfg.qos = atoi(val);
        } else if (strcmp(section, "database") == 0) {
            if (strcmp(key, "path") == 0) strncpy(g_cfg.db_path, val, 63);
        } else if (strncmp(section, "device_", 7) == 0) {
            if (strcmp(section, last_section) != 0) {
                strncpy(last_section, section, 63);
                last_idx = g_cfg.device_count;
                if (last_idx < DEVICE_MAX) {
                    g_cfg.device_count++;
                    g_cfg.devices[last_idx].slave_id     = 1;
                    g_cfg.devices[last_idx].reg_temp     = 0;
                    g_cfg.devices[last_idx].reg_humidity = 1;
                    strncpy(g_cfg.devices[last_idx].topic,
                            "factory/sensor/unknown", 127);
                }
            }
            if (last_idx >= 0 && last_idx < DEVICE_MAX) {
                if      (strcmp(key, "slave_id")     == 0)
                    g_cfg.devices[last_idx].slave_id = atoi(val);
                else if (strcmp(key, "reg_temp")     == 0)
                    g_cfg.devices[last_idx].reg_temp = atoi(val);
                else if (strcmp(key, "reg_humidity") == 0)
                    g_cfg.devices[last_idx].reg_humidity = atoi(val);
                else if (strcmp(key, "topic")        == 0)
                    strncpy(g_cfg.devices[last_idx].topic, val, 127);
            }
        }
    }
    fclose(f);
    log_write(LOG_INFO, "[CFG] 配置加载成功: %s，共%d个设备", path, g_cfg.device_count);
}

/* ─────────────── 队列 ─────────────── */
#define QUEUE_MAX 32
#define MSG_LEN   256

typedef struct {
    char topic[128];
    char payload[MSG_LEN];
} Message;

typedef struct {
    Message items[QUEUE_MAX];
    int head, tail, count;
    pthread_mutex_t lock;
    pthread_cond_t  not_empty;
    pthread_cond_t  not_full;
} SafeQueue;

SafeQueue g_queue;

void queue_init(SafeQueue *q) {
    q->head = q->tail = q->count = 0;
    pthread_mutex_init(&q->lock, NULL);
    pthread_cond_init(&q->not_empty, NULL);
    pthread_cond_init(&q->not_full, NULL);
}

/*
 * 修复：队列满时改用带超时的等待，每秒检查一次 g_running。
 * MQTT 长时间断线时 collect_thread 不再永久卡死。
 * 返回 0 成功，-1 表示程序正在退出（消息丢弃）。
 */
static int queue_push(SafeQueue *q, const char *topic, const char *payload) {
    pthread_mutex_lock(&q->lock);
    while (q->count == QUEUE_MAX && g_running) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 1;
        pthread_cond_timedwait(&q->not_full, &q->lock, &ts);
    }
    if (!g_running) {
        pthread_mutex_unlock(&q->lock);
        return -1;
    }
    strncpy(q->items[q->tail].topic,   topic,   127);
    strncpy(q->items[q->tail].payload, payload, MSG_LEN - 1);
    q->items[q->tail].topic[127]        = '\0';
    q->items[q->tail].payload[MSG_LEN-1] = '\0';
    q->tail = (q->tail + 1) % QUEUE_MAX;
    q->count++;
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
    return 0;
}

/* 修复：带超时等待，返回 1 表示取到消息，0 表示程序退出 */
static int queue_pop(SafeQueue *q, Message *out) {
    pthread_mutex_lock(&q->lock);
    while (q->count == 0 && g_running) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 1;
        pthread_cond_timedwait(&q->not_empty, &q->lock, &ts);
    }
    int got = 0;
    if (q->count > 0) {
        *out = q->items[q->head];
        q->head = (q->head + 1) % QUEUE_MAX;
        q->count--;
        pthread_cond_signal(&q->not_full);
        got = 1;
    }
    pthread_mutex_unlock(&q->lock);
    return got;
}

/* ─────────────── SQLite ─────────────── */
sqlite3 *g_db;

void db_init(void) {
    /* 修复：检查返回值 */
    if (sqlite3_open(g_cfg.db_path, &g_db) != SQLITE_OK) {
        log_write(LOG_ERROR, "[DB] 打开数据库失败: %s", sqlite3_errmsg(g_db));
        exit(1);
    }
    sqlite3_exec(g_db,
        "CREATE TABLE IF NOT EXISTS queue ("
        "id      INTEGER PRIMARY KEY AUTOINCREMENT,"
        "topic   TEXT NOT NULL,"
        "payload TEXT NOT NULL,"
        "sent    INTEGER DEFAULT 0);",
        NULL, NULL, NULL);
}

void db_save(const char *topic, const char *payload) {
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(g_db,
            "INSERT INTO queue (topic, payload) VALUES (?, ?);",
            -1, &stmt, NULL) != SQLITE_OK) {
        log_write(LOG_ERROR, "[DB] 缓存失败: %s", sqlite3_errmsg(g_db));
        return;
    }
    sqlite3_bind_text(stmt, 1, topic,   -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, payload, -1, SQLITE_STATIC);
    if (sqlite3_step(stmt) != SQLITE_DONE)
        log_write(LOG_ERROR, "[DB] 写入失败: %s", sqlite3_errmsg(g_db));
    else
        log_write(LOG_WARN, "[DB] 缓存: %s", payload);
    sqlite3_finalize(stmt);
}

/*
 * 修复1：先将列值复制到本地 buf，避免 sqlite3_step 推进后指针失效。
 * 修复2：检查 MQTT 发布返回值，失败时立即停止补发并返回 -1。
 * 修复3：补发成功后 DELETE 而非 UPDATE sent=1，防止数据库无限增长。
 */
static int db_replay(MQTTClient client) {
    sqlite3_stmt *stmt;
    /* 修复：检查 prepare 返回值，失败则 stmt 为 NULL，step 会崩溃 */
    if (sqlite3_prepare_v2(g_db,
            "SELECT id, topic, payload FROM queue WHERE sent=0 ORDER BY id;",
            -1, &stmt, NULL) != SQLITE_OK) {
        log_write(LOG_ERROR, "[DB] prepare 失败: %s", sqlite3_errmsg(g_db));
        return 0;
    }

    int all_ok = 1;
    while (sqlite3_step(stmt) == SQLITE_ROW && g_running) {
        int  id = sqlite3_column_int(stmt, 0);
        char topic[128], payload[MSG_LEN];
        strncpy(topic,   (const char *)sqlite3_column_text(stmt, 1), 127);
        strncpy(payload, (const char *)sqlite3_column_text(stmt, 2), MSG_LEN - 1);
        topic[127]        = '\0';
        payload[MSG_LEN-1] = '\0';

        MQTTClient_message msg = MQTTClient_message_initializer;
        msg.payload    = payload;
        msg.payloadlen = (int)strlen(payload);
        msg.qos        = g_cfg.qos;
        msg.retained   = 0;

        MQTTClient_deliveryToken token;
        int rc = MQTTClient_publishMessage(client, topic, &msg, &token);
        if (rc != MQTTCLIENT_SUCCESS) {
            log_write(LOG_WARN, "[DB] 补发失败，留待下次重连");
            all_ok = 0;
            break;
        }
        /* 修复：waitForCompletion 失败（超时/断线）则不删除记录，留待下次 */
        if (MQTTClient_waitForCompletion(client, token, 5000L) != MQTTCLIENT_SUCCESS) {
            log_write(LOG_WARN, "[DB] 补发等待超时，留待下次重连");
            all_ok = 0;
            break;
        }

        char sql[64];
        snprintf(sql, sizeof(sql), "DELETE FROM queue WHERE id=%d;", id);
        sqlite3_exec(g_db, sql, NULL, NULL, NULL);
        log_write(LOG_INFO, "[DB] 补发: %s", payload);
    }
    sqlite3_finalize(stmt);
    return all_ok;
}

/* ─────────────── 采集线程 ─────────────── */
#define MODBUS_MAX_FAIL 5   /* 连续失败超过此次数触发重连 */

void *collect_thread(void *arg) {
    (void)arg;
    /* 修复：检查 modbus_new_rtu 返回值 */
    modbus_t *ctx = modbus_new_rtu(g_cfg.serial_port, g_cfg.baudrate, 'E', 8, 1);
    if (!ctx) {
        log_write(LOG_ERROR, "[Modbus] modbus_new_rtu 失败");
        return NULL;
    }
    /* 设置响应超时，防止设备无响应时线程卡死 */
    modbus_set_response_timeout(ctx, 1, 0);

    while (g_running && modbus_connect(ctx) < 0) {
        log_write(LOG_WARN, "[Modbus] 串口连接失败，1秒后重试...");
        sleep(1);
    }
    if (!g_running) { modbus_free(ctx); return NULL; }
    log_write(LOG_INFO, "[Modbus] 串口连接成功，共%d个设备", g_cfg.device_count);

    /* bus_fail 统计整轮全部设备都失败的连续轮次，用于判断串口是否断线。
     * 单台设备失败不影响计数，避免一台在线设备掩盖另一台设备的持续失败。 */
    int bus_fail   = 0;
    int stat_round = 0;   /* 每 60 轮打印一次各设备统计 */
    while (g_running) {
        int cycle_ok = 0;   /* 本轮是否至少有一台设备读取成功 */

        for (int i = 0; i < g_cfg.device_count && g_running; i++) {
            DeviceConfig *dev = &g_cfg.devices[i];
            modbus_set_slave(ctx, dev->slave_id);

            uint16_t reg_t = 0, reg_h = 0;
            int rc1 = modbus_read_registers(ctx, dev->reg_temp,    1, &reg_t);
            int rc2 = modbus_read_registers(ctx, dev->reg_humidity, 1, &reg_h);

            if (rc1 == 1 && rc2 == 1) {
                cycle_ok = 1;
                dev->ok_count++;
                char payload[MSG_LEN];
                snprintf(payload, sizeof(payload),
                         "{\"ts\":%ld,\"temp\":%.1f,\"humidity\":%.1f}",
                         (long)time(NULL), reg_t / 10.0f, reg_h / 10.0f);
                queue_push(&g_queue, dev->topic, payload);
                log_write(LOG_INFO, "[Modbus] 设备%d: %s", dev->slave_id, payload);
            } else {
                dev->fail_count++;
                log_write(LOG_WARN, "[Modbus] 设备%d 读取失败", dev->slave_id);
            }
        }

        if (++stat_round >= 60) {
            stat_round = 0;
            for (int i = 0; i < g_cfg.device_count; i++)
                log_write(LOG_INFO, "[STAT] 设备%d: 成功%u次 失败%u次",
                          g_cfg.devices[i].slave_id,
                          g_cfg.devices[i].ok_count,
                          g_cfg.devices[i].fail_count);
        }

        /* 只有整轮全部失败才计入总线失败次数并考虑重连 */
        if (g_cfg.device_count > 0 && !cycle_ok) {
            bus_fail++;
            log_write(LOG_WARN, "[Modbus] 全部设备失败（连续%d轮）", bus_fail);
            if (bus_fail >= MODBUS_MAX_FAIL) {
                log_write(LOG_WARN, "[Modbus] 连续%d轮全部失败，重连串口...",
                          MODBUS_MAX_FAIL);
                modbus_close(ctx);
                while (g_running && modbus_connect(ctx) < 0) {
                    log_write(LOG_WARN, "[Modbus] 重连失败，1秒后重试...");
                    sleep(1);
                }
                if (g_running) {
                    log_write(LOG_INFO, "[Modbus] 串口重连成功");
                    bus_fail = 0;
                }
            }
        } else {
            bus_fail = 0;
        }

        if (g_running) sleep(g_cfg.poll_interval);
    }

    modbus_close(ctx);
    modbus_free(ctx);
    log_write(LOG_INFO, "[Modbus] 采集线程退出");
    return NULL;
}

/* 发布网关在线状态（retained），供订阅方感知存活；重连后调用可覆盖 LWT 的 offline */
static void publish_online_status(MQTTClient client) {
    MQTTClient_message om = MQTTClient_message_initializer;
    om.payload    = "online";
    om.payloadlen = 6;
    om.qos        = 1;
    om.retained   = 1;
    MQTTClient_deliveryToken tok;
    MQTTClient_publishMessage(client, "gateway/status", &om, &tok);
    MQTTClient_waitForCompletion(client, tok, 3000L);
}

/* 校验配置合法性，将非法值重置为安全默认值 */
static void config_validate(void) {
    if (g_cfg.poll_interval <= 0) {
        log_write(LOG_WARN, "[CFG] poll_interval=%d 无效，重置为1", g_cfg.poll_interval);
        g_cfg.poll_interval = 1;
    }
    if (g_cfg.baudrate <= 0) {
        log_write(LOG_WARN, "[CFG] baudrate=%d 无效，重置为9600", g_cfg.baudrate);
        g_cfg.baudrate = 9600;
    }
    if (g_cfg.qos < 0 || g_cfg.qos > 2) {
        log_write(LOG_WARN, "[CFG] qos=%d 无效，重置为1", g_cfg.qos);
        g_cfg.qos = 1;
    }
    if (g_cfg.device_count == 0)
        log_write(LOG_WARN, "[CFG] 未配置任何设备");
}

/* ─────────────── 发布线程 ─────────────── */
void *publish_thread(void *arg) {
    (void)arg;
    MQTTClient client;
    MQTTClient_connectOptions opts = MQTTClient_connectOptions_initializer;
    opts.keepAliveInterval = 20;
    /* 修复：cleansession=0 与 QoS 1 配套，Broker 侧保留会话以保证投递 */
    opts.cleansession = 0;

    /* 修复：设置 LWT，网关异常离线时 Broker 自动发布离线通知 */
    MQTTClient_willOptions will = MQTTClient_willOptions_initializer;
    will.topicName = "gateway/status";
    will.message   = "offline";
    will.qos       = 1;
    will.retained  = 1;
    opts.will      = &will;

    if (MQTTClient_create(&client, g_cfg.broker, g_cfg.client_id,
                          MQTTCLIENT_PERSISTENCE_NONE, NULL) != MQTTCLIENT_SUCCESS) {
        log_write(LOG_ERROR, "[MQTT] MQTTClient_create 失败");
        return NULL;
    }

    while (g_running && MQTTClient_connect(client, &opts) != MQTTCLIENT_SUCCESS) {
        log_write(LOG_WARN, "[MQTT] 连接失败，1秒后重试...");
        sleep(1);
    }
    if (!g_running) { MQTTClient_destroy(&client); return NULL; }
    log_write(LOG_INFO, "[MQTT] 连接Broker成功");

    publish_online_status(client);
    db_replay(client);

    Message msg;
    while (g_running) {
        if (!queue_pop(&g_queue, &msg)) continue;

        MQTTClient_message mqtt_msg = MQTTClient_message_initializer;
        mqtt_msg.payload    = msg.payload;
        mqtt_msg.payloadlen = (int)strlen(msg.payload);
        mqtt_msg.qos        = g_cfg.qos;
        mqtt_msg.retained   = 0;

        MQTTClient_deliveryToken token;
        int rc = MQTTClient_publishMessage(client, msg.topic, &mqtt_msg, &token);
        int need_reconnect = 0;

        if (rc == MQTTCLIENT_SUCCESS) {
            /* 修复：检查 waitForCompletion，超时视为投递失败，缓存重发 */
            if (MQTTClient_waitForCompletion(client, token, 10000L) == MQTTCLIENT_SUCCESS) {
                log_write(LOG_INFO, "[MQTT] 发布 [%s]: %s", msg.topic, msg.payload);
            } else {
                log_write(LOG_WARN, "[MQTT] 发布确认超时，缓存重发: %s", msg.topic);
                db_save(msg.topic, msg.payload);
                need_reconnect = 1;
            }
        } else {
            db_save(msg.topic, msg.payload);
            need_reconnect = 1;
        }

        if (need_reconnect) {
            /* 先 disconnect 再 connect，避免 paho 返回 MQTTCLIENT_CONNECTED
             * 导致重连循环无法退出 */
            MQTTClient_disconnect(client, 0);
            while (g_running && MQTTClient_connect(client, &opts) != MQTTCLIENT_SUCCESS) {
                log_write(LOG_WARN, "[MQTT] 重连中...");
                sleep(2);
            }
            if (g_running) {
                log_write(LOG_INFO, "[MQTT] 重连成功");
                publish_online_status(client);
                db_replay(client);
            }
        }
    }

    /* 修复：优雅退出时将队列中剩余消息落地到 SQLite，防止丢失 */
    {
        Message drain;
        while (queue_pop(&g_queue, &drain))
            db_save(drain.topic, drain.payload);
    }

    MQTTClient_destroy(&client);
    log_write(LOG_INFO, "[MQTT] 发布线程退出");
    return NULL;
}

/* ─────────────── main ─────────────── */
int main(void) {
    /* 修复：注册信号，支持 Ctrl-C / systemd stop 优雅退出 */
    signal(SIGINT,  sig_handler);
    signal(SIGTERM, sig_handler);

    log_write(LOG_INFO, "=== Modbus->MQTT 网关启动 ===");

    config_load("gateway.conf");
    config_validate();
    db_init();
    queue_init(&g_queue);

    pthread_t t_collect, t_publish;
    /* 修复：检查 pthread_create 返回值 */
    if (pthread_create(&t_collect, NULL, collect_thread, NULL) != 0) {
        log_write(LOG_ERROR, "创建采集线程失败");
        return 1;
    }
    if (pthread_create(&t_publish, NULL, publish_thread, NULL) != 0) {
        log_write(LOG_ERROR, "创建发布线程失败");
        g_running = 0;
        pthread_join(t_collect, NULL);
        return 1;
    }

    pthread_join(t_collect, NULL);
    pthread_join(t_publish, NULL);

    sqlite3_close(g_db);
    log_write(LOG_INFO, "=== 网关已退出 ===");
    return 0;
}

