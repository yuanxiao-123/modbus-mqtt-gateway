#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <modbus/modbus.h>
#include <MQTTClient.h>
#include <sqlite3.h>
#include <ctype.h>
#include <stdarg.h>
#include <time.h>

/* ─────────────── 日志 ─────────────── */
typedef enum {
    LOG_INFO,
    LOG_WARN,
    LOG_ERROR
} LogLevel;

void log_write(LogLevel level, const char *fmt, ...) {
    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    char timebuf[32];
    strftime(timebuf, sizeof(timebuf), "%Y-%m-%d %H:%M:%S", t);

    const char *level_str;
    switch (level) {
        case LOG_INFO:  level_str = "INFO "; break;
        case LOG_WARN:  level_str = "WARN "; break;
        case LOG_ERROR: level_str = "ERROR"; break;
        default:        level_str = "INFO "; break;
    }

    printf("[%s][%s] ", timebuf, level_str);

    va_list args;
    va_start(args, fmt);
    vprintf(fmt, args);
    va_end(args);
    printf("\n");
}

/* ─────────────── 配置 ─────────────── */
typedef struct {
    char serial_port[64];
    int  baudrate;
    int  slave_id;
    int  poll_interval;
    char broker[128];
    char client_id[64];
    char topic[128];
    int  qos;
    char db_path[64];
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
    strncpy(g_cfg.topic,       "factory/sensor/01",    127);
    strncpy(g_cfg.db_path,     "gateway.db",            63);
    g_cfg.baudrate      = 9600;
    g_cfg.slave_id      = 1;
    g_cfg.poll_interval = 1;
    g_cfg.qos           = 1;

    FILE *f = fopen(path, "r");
    if (!f) {
        log_write(LOG_WARN, "[CFG] 配置文件不存在，使用默认值");
        return;
    }

    char line[256], section[64] = "";
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
            else if (strcmp(key, "slave_id")      == 0) g_cfg.slave_id      = atoi(val);
            else if (strcmp(key, "poll_interval") == 0) g_cfg.poll_interval = atoi(val);
        } else if (strcmp(section, "mqtt") == 0) {
            if      (strcmp(key, "broker")    == 0) strncpy(g_cfg.broker,    val, 127);
            else if (strcmp(key, "client_id") == 0) strncpy(g_cfg.client_id, val,  63);
            else if (strcmp(key, "topic")     == 0) strncpy(g_cfg.topic,     val, 127);
            else if (strcmp(key, "qos")       == 0) g_cfg.qos = atoi(val);
        } else if (strcmp(section, "database") == 0) {
            if (strcmp(key, "path") == 0) strncpy(g_cfg.db_path, val, 63);
        }
    }
    fclose(f);
    log_write(LOG_INFO, "[CFG] 配置加载成功: %s", path);
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

void queue_push(SafeQueue *q, const char *topic, const char *payload) {
    pthread_mutex_lock(&q->lock);
    while (q->count == QUEUE_MAX)
        pthread_cond_wait(&q->not_full, &q->lock);
    strncpy(q->items[q->tail].topic,   topic,   127);
    strncpy(q->items[q->tail].payload, payload, MSG_LEN - 1);
    q->tail = (q->tail + 1) % QUEUE_MAX;
    q->count++;
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
}

void queue_pop(SafeQueue *q, Message *out) {
    pthread_mutex_lock(&q->lock);
    while (q->count == 0)
        pthread_cond_wait(&q->not_empty, &q->lock);
    *out = q->items[q->head];
    q->head = (q->head + 1) % QUEUE_MAX;
    q->count--;
    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->lock);
}

/* ─────────────── SQLite ─────────────── */
sqlite3 *g_db;

void db_init(void) {
    sqlite3_open(g_cfg.db_path, &g_db);
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
    sqlite3_prepare_v2(g_db,
        "INSERT INTO queue (topic, payload) VALUES (?, ?);",
        -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, topic,   -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, payload, -1, SQLITE_STATIC);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    log_write(LOG_WARN, "[DB] 缓存: %s", payload);
}

void db_replay(MQTTClient client) {
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(g_db,
        "SELECT id, topic, payload FROM queue WHERE sent=0 ORDER BY id;",
        -1, &stmt, NULL);

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        int         id      = sqlite3_column_int (stmt, 0);
        const char *topic   = (const char *)sqlite3_column_text(stmt, 1);
        const char *payload = (const char *)sqlite3_column_text(stmt, 2);

        MQTTClient_message msg = MQTTClient_message_initializer;
        msg.payload    = (void *)payload;
        msg.payloadlen = strlen(payload);
        msg.qos        = g_cfg.qos;
        msg.retained   = 0;

        MQTTClient_deliveryToken token;
        MQTTClient_publishMessage(client, topic, &msg, &token);
        MQTTClient_waitForCompletion(client, token, 10000L);

        char sql[64];
        snprintf(sql, sizeof(sql),
                 "UPDATE queue SET sent=1 WHERE id=%d;", id);
        sqlite3_exec(g_db, sql, NULL, NULL, NULL);
        log_write(LOG_INFO, "[DB] 补发: %s", payload);
    }
    sqlite3_finalize(stmt);
}

/* ─────────────── 采集线程 ─────────────── */
void *collect_thread(void *arg) {
    modbus_t *ctx = modbus_new_rtu(g_cfg.serial_port, g_cfg.baudrate, 'N', 8, 1);
    modbus_set_slave(ctx, g_cfg.slave_id);

    while (modbus_connect(ctx) < 0) {
        log_write(LOG_WARN, "[Modbus] 串口连接失败，1秒后重试...");
        sleep(1);
    }
    log_write(LOG_INFO, "[Modbus] 串口连接成功");

    uint16_t regs[2];
    while (1) {
        int rc = modbus_read_registers(ctx, 0, 2, regs);
        if (rc == 2) {
            char payload[MSG_LEN];
            snprintf(payload, sizeof(payload),
                     "{\"temp\":%.1f,\"humidity\":%.1f}",
                     regs[0] / 10.0f, regs[1] / 10.0f);
            queue_push(&g_queue, g_cfg.topic, payload);
            log_write(LOG_INFO, "[Modbus] 采集: %s", payload);
        } else {
            log_write(LOG_WARN, "[Modbus] 读取失败，跳过");
        }
        sleep(g_cfg.poll_interval);
    }
    modbus_close(ctx);
    modbus_free(ctx);
    return NULL;
}

/* ─────────────── 发布线程 ─────────────── */
void *publish_thread(void *arg) {
    MQTTClient client;
    MQTTClient_connectOptions opts = MQTTClient_connectOptions_initializer;
    opts.keepAliveInterval = 20;
    opts.cleansession      = 1;

    MQTTClient_create(&client, g_cfg.broker, g_cfg.client_id,
                      MQTTCLIENT_PERSISTENCE_NONE, NULL);

    while (MQTTClient_connect(client, &opts) != MQTTCLIENT_SUCCESS) {
        log_write(LOG_WARN, "[MQTT] 连接失败，1秒后重试...");
        sleep(1);
    }
    log_write(LOG_INFO, "[MQTT] 连接Broker成功");

    db_replay(client);

    Message msg;
    while (1) {
        queue_pop(&g_queue, &msg);

        MQTTClient_message mqtt_msg = MQTTClient_message_initializer;
        mqtt_msg.payload    = msg.payload;
        mqtt_msg.payloadlen = strlen(msg.payload);
        mqtt_msg.qos        = g_cfg.qos;
        mqtt_msg.retained   = 0;

        MQTTClient_deliveryToken token;
        int rc = MQTTClient_publishMessage(client, g_cfg.topic,
                                           &mqtt_msg, &token);

        if (rc == MQTTCLIENT_SUCCESS) {
            MQTTClient_waitForCompletion(client, token, 10000L);
            log_write(LOG_INFO, "[MQTT] 发布: %s", msg.payload);
        } else {
            db_save(msg.topic, msg.payload);

            while (MQTTClient_connect(client, &opts) != MQTTCLIENT_SUCCESS) {
                log_write(LOG_WARN, "[MQTT] 重连中...");
                sleep(2);
            }
            log_write(LOG_INFO, "[MQTT] 重连成功");
            db_replay(client);
        }
    }

    MQTTClient_destroy(&client);
    return NULL;
}

/* ─────────────── main ─────────────── */
int main(void) {
    log_write(LOG_INFO, "=== Modbus→MQTT 网关启动 ===");

    config_load("gateway.conf");
    db_init();
    queue_init(&g_queue);

    pthread_t t_collect, t_publish;
    pthread_create(&t_collect, NULL, collect_thread, NULL);
    pthread_create(&t_publish, NULL, publish_thread, NULL);

    pthread_join(t_collect, NULL);
    pthread_join(t_publish, NULL);
    return 0;
}

