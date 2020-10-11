/*
 * pg_kafka - PostgreSQL extension to produce messages to Apache Kafka
 *
 * Copyright (c) 2014 Xavier Stevens (c) 2020 Marc-Antoine Parent
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include <string.h>
#include <syslog.h>
#include <sys/time.h>
#include <errno.h>
#include <search.h>

#include "postgres.h"
#include "fmgr.h"
#include "executor/spi.h"
#include "access/xact.h"
#include "utils/builtins.h"
#include "librdkafka/rdkafka.h"

PG_MODULE_MAGIC;

void _PG_init(void);
Datum pg_kafka_produce(PG_FUNCTION_ARGS);
Datum pg_kafka_close(PG_FUNCTION_ARGS);

/**
 * Message delivery report callback.
 * Called once for each message.
 * See rkafka.h for more information.
 */
static void rk_msg_delivered(rd_kafka_t *rk, void *payload, size_t len,
                             int error_code, void *opaque, void *msg_opaque) {
  if (error_code)
    elog(WARNING, "%% Message delivery failed: %s\n",
         rd_kafka_err2str(error_code));
  fprintf(stderr, "Messsage delivery success.\n");
}

/**
 * Kafka logger callback
 */
static void rk_logger(const rd_kafka_t *rk, int level, const char *fac,
                      const char *buf) {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  fprintf(stderr, "%u.%03u RDKAFKA-%i-%s: %s: %s\n", (int)tv.tv_sec,
          (int)(tv.tv_usec / 1000), level, fac, rd_kafka_name(rk), buf);
}

static rd_kafka_t *GRK = NULL;

static rd_kafka_t *get_rk() {
  if (GRK) {
    return GRK;
  }

  rd_kafka_t *rk;
  char errstr[512];
  char *brokers;
  char *sql = "select string_agg(host || ':' || port, ',') from kafka.broker";
  if (SPI_connect() == SPI_ERROR_CONNECT)
    return NULL;
  if (SPI_OK_SELECT == SPI_execute(sql, true, 100) && SPI_processed > 0) {
    brokers = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
  } else {
    brokers = "localhost:9092";
  }
  SPI_finish();

  /* kafka configuration */
  rd_kafka_conf_t *conf = rd_kafka_conf_new();

  /* add brokers */
  if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr,
                        sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    elog(WARNING, "%s\n", errstr);
    return NULL;
  }

  if (rd_kafka_conf_set(conf, "compression.codec", "snappy", errstr,
                        sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    elog(WARNING, "%s\n", errstr);
  }

  /* set message delivery callback */
  rd_kafka_conf_set_dr_cb(conf, rk_msg_delivered);

  /* get producer handle to kafka */
  if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr)))) {
    elog(WARNING, "%% Failed to create new producer: %s\n", errstr);
    goto broken;
  }

  /* set logger */
  rd_kafka_conf_set_log_cb(conf, rk_logger);
  rd_kafka_set_log_level(rk, LOG_INFO);

  GRK = rk;
  return rk;
broken:
  rd_kafka_destroy(rk);
  return NULL;
}

typedef struct msg_que_elem {
  struct msg_que_elem *next;
  struct msg_que_elem *prev;
  void* msg;
  size_t msg_len;
} msg_que_elem;


typedef struct topic_record {
  const char* name;
  rd_kafka_topic_t* topic;
  msg_que_elem* msg_queue;
} topic_record;

static int compare_topic_records(const void* a, const void* b) {
  return strcmp(((topic_record*)a)->name, ((topic_record*)b)->name);
}


static void* TOPICS = NULL;

static topic_record* get_topic_record(rd_kafka_t *rk, const char* name) {
  topic_record search_record = {name, NULL};
  topic_record** node = tfind(&search_record, &TOPICS, &compare_topic_records);
  if (node == NULL) {
    topic_record* record = malloc(sizeof(topic_record));
    record->name = strdup(name);
    rd_kafka_topic_conf_t *topic_conf = rd_kafka_topic_conf_new();
    record->topic = rd_kafka_topic_new(rk, name, topic_conf);
    record->msg_queue = NULL;
    node = tsearch(record, &TOPICS, &compare_topic_records);
  }
  return *node;
}


static topic_record* TOPIC_LEAF = NULL;

static void record_leaf(const void* nodep, VISIT which, int level) {
  if (which == leaf) {
    TOPIC_LEAF = *(topic_record**)nodep;
  }
}


static void empty_topic_queue(topic_record* rec) {
  while (rec->msg_queue != NULL) {
    msg_que_elem* node = rec->msg_queue;
    free(node->msg);
    rec->msg_queue = node->prev;
    remque(node);
  }
}

static void empty_topic_node_queue(const void* recnode, VISIT which, int level) {
  empty_topic_queue(*(topic_record*const*)recnode);
}

static void empty_all_topic_queues() {
  twalk(TOPICS, &empty_topic_node_queue);
}

static void send_message_topic(const void* recnode, VISIT which, int level) {
  topic_record* record = *(topic_record*const*)recnode;
  msg_que_elem* node = record->msg_queue;
  // go to the beginning of the queue and walk it forward
  while (node != NULL && node->prev != NULL)
    node = node->prev;
  while (node != NULL) {
    /* using random partition for now */
    int partition = RD_KAFKA_PARTITION_UA;

    /* send/produce message. */
    int rv = rd_kafka_produce(
      record->topic, partition, RD_KAFKA_MSG_F_FREE,
      node->msg, node->msg_len,
      NULL, 0, NULL);
    if (rv == -1) {
      fprintf(stderr, "%% Failed to produce to topic %s partition %i: %s\n",
              rd_kafka_topic_name(record->topic), partition,
              rd_kafka_err2str(rd_kafka_last_error()));
      /* poll to handle delivery reports */
      // rd_kafka_poll(rk, 0);
    }
    msg_que_elem* next_node = node->next;
    remque(node);
    node = next_node;
  }
  record->msg_queue = NULL;
}


static void send_all_message_topics() {
  twalk(TOPICS, &send_message_topic);
}


static void delete_topics() {
  while (true) {
    // find any one leaf
    TOPIC_LEAF = NULL;
    twalk(TOPICS, &record_leaf);
    if (TOPIC_LEAF == NULL) break;
    free((void*)TOPIC_LEAF->name);
    empty_topic_queue(TOPIC_LEAF);
    rd_kafka_topic_destroy(TOPIC_LEAF->topic);
    tdelete(TOPIC_LEAF, &TOPICS, &compare_topic_records);
  }
  TOPICS = NULL;
}


PG_FUNCTION_INFO_V1(pg_kafka_flush);
Datum pg_kafka_flush(PG_FUNCTION_ARGS) {
  rd_kafka_t *rk = get_rk();
  rd_kafka_flush(rk, 1000);
  //empty_all_topic_queues();
  PG_RETURN_VOID();
}


static void rk_destroy() {
  rd_kafka_t *rk = get_rk();
  rd_kafka_flush(rk, 1000);
  delete_topics();
  rd_kafka_destroy(rk);
  GRK = NULL;
}

static void pg_xact_callback(XactEvent event, void *arg) {
  switch (event) {
    case XACT_EVENT_COMMIT:
    case XACT_EVENT_PARALLEL_COMMIT:
      send_all_message_topics();
      break;
    case XACT_EVENT_ABORT:
    case XACT_EVENT_PARALLEL_ABORT:
      empty_all_topic_queues();
      break;
    case XACT_EVENT_PREPARE:
      /* nothin' */
      break;
    case XACT_EVENT_PRE_COMMIT:
    case XACT_EVENT_PARALLEL_PRE_COMMIT:
      /* nothin' */
      break;
    case XACT_EVENT_PRE_PREPARE:
      /* nothin' */
      break;
  }
}

void _PG_init() { RegisterXactCallback(pg_xact_callback, NULL); }

PG_FUNCTION_INFO_V1(pg_kafka_produce);
Datum pg_kafka_produce(PG_FUNCTION_ARGS) {
  if (!PG_ARGISNULL(0) && !PG_ARGISNULL(1)) {
    /* get topic arg */
    text *topic_txt = PG_GETARG_TEXT_PP(0);
    char *topic = text_to_cstring(topic_txt);
    /* get msg arg */
    text *msg_txt = PG_GETARG_TEXT_PP(1);
    void *msg = VARDATA_ANY(msg_txt);
    size_t msg_len = VARSIZE_ANY_EXHDR(msg_txt);
    /* create topic */
    rd_kafka_t *rk = get_rk();
    if (!rk) {
      PG_RETURN_BOOL(0 != 0);
    }
    topic_record *trec = get_topic_record(rk, topic);
    msg_que_elem* node = malloc(sizeof(msg_que_elem));
    node->prev = node->next = NULL;
    node->msg = malloc(msg_len+1);
    memcpy(node->msg, msg, msg_len);
    ((char*)(node->msg))[msg_len] = 0;
    node->msg_len = msg_len;
    insque(node, trec->msg_queue);
    trec->msg_queue = node;
    PG_RETURN_BOOL(1 == 1);
  }
  PG_RETURN_BOOL(0 != 0);
}

PG_FUNCTION_INFO_V1(pg_kafka_close);
Datum pg_kafka_close(PG_FUNCTION_ARGS) {
  rk_destroy();
  PG_RETURN_VOID();
}
