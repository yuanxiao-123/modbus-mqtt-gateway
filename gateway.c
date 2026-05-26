#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <modbus/modbus.h>
#include <MQTTClient.h>
#include <sqlite3.h>

/* ─────────────── 配置 ─────────────── */
#define SERIAL_PORT   "/dev/pts/2"
#define SLAVE_ID      1
#define POLL_INTERVAL 1          // 采集间隔（秒）

#define BROKER_URL    "tcp://localhost:1883"
#define CLIENT_ID     "modbus_mqtt_gateway"
#define TOPIC         "factory/sensor/01"
#define QOS           1
#define TIMEOUT       10000L

#define DB_PATH       "gateway.db"

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
    sqlite3_open(DB_PATH, &g_db);
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
    printf("[DB] 缓存: %s\n", payload);
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
        msg.qos        = QOS;
        msg.retained   = 0;

        MQTTClient_deliveryToken token;
        MQTTClient_publishMessage(client, topic, &msg, &token);
        MQTTClient_waitForCompletion(client, token, TIMEOUT);

        // 标记已发送
        char sql[64];
        snprintf(sql, sizeof(sql),
                 "UPDATE queue SET sent=1 WHERE id=%d;", id);
        sqlite3_exec(g_db, sql, NULL, NULL, NULL);
        printf("[DB] 补发: %s\n", payload);
    }
    sqlite3_finalize(stmt);
}

/* ─────────────── 采集线程 ─────────────── */
void *collect_thread(void *arg) {
    modbus_t *ctx = modbus_new_rtu(SERIAL_PORT, 9600, 'N', 8, 1);
    modbus_set_slave(ctx, SLAVE_ID);

    while (modbus_connect(ctx) < 0) {
        printf("[Modbus] 串口连接失败，1秒后重试...\n");
        sleep(1);
    }
    printf("[Modbus] 串口连接成功\n");

    uint16_t regs[2];
    while (1) {
        int rc = modbus_read_registers(ctx, 0, 2, regs);
        if (rc == 2) {
            char payload[MSG_LEN];
            snprintf(payload, sizeof(payload),
                     "{\"temp\":%.1f,\"humidity\":%.1f}",
                     regs[0] / 10.0f, regs[1] / 10.0f);
            queue_push(&g_queue, TOPIC, payload);
            printf("[Modbus] 采集: %s\n", payload);
        } else {
            printf("[Modbus] 读取失败，跳过\n");
        }
        sleep(POLL_INTERVAL);
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

    MQTTClient_create(&client, BROKER_URL, CLIENT_ID,
                      MQTTCLIENT_PERSISTENCE_NONE, NULL);

    // 连接Broker，失败则重试
    while (MQTTClient_connect(client, &opts) != MQTTCLIENT_SUCCESS) {
        printf("[MQTT] 连接失败，1秒后重试...\n");
        sleep(1);
    }
    printf("[MQTT] 连接Broker成功\n");

    // 重连后补发缓存数据
    db_replay(client);

    Message msg;
    while (1) {
        queue_pop(&g_queue, &msg);

        MQTTClient_message mqtt_msg = MQTTClient_message_initializer;
        mqtt_msg.payload    = msg.payload;
        mqtt_msg.payloadlen = strlen(msg.payload);
        mqtt_msg.qos        = QOS;
        mqtt_msg.retained   = 0;

        MQTTClient_deliveryToken token;
        int rc = MQTTClient_publishMessage(client, msg.topic,
                                           &mqtt_msg, &token);

        if (rc == MQTTCLIENT_SUCCESS) {
            MQTTClient_waitForCompletion(client, token, TIMEOUT);
            printf("[MQTT] 发布: %s\n", msg.payload);
        } else {
            // 发布失败，存入SQLite
            db_save(msg.topic, msg.payload);

            // 等待重连
            while (MQTTClient_connect(client, &opts) != MQTTCLIENT_SUCCESS) {
                printf("[MQTT] 重连中...\n");
                sleep(2);
            }
            printf("[MQTT] 重连成功\n");
            db_replay(client);
        }
    }

    MQTTClient_destroy(&client);
    return NULL;
}

/* ─────────────── main ─────────────── */
int main(void) {
    printf("=== Modbus→MQTT 网关启动 ===\n");

    db_init();
    queue_init(&g_queue);

    pthread_t t_collect, t_publish;
    pthread_create(&t_collect, NULL, collect_thread, NULL);
    pthread_create(&t_publish, NULL, publish_thread, NULL);

    pthread_join(t_collect, NULL);
    pthread_join(t_publish, NULL);
    return 0;
}
