/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2020, Magnus Edenhill
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "test.h"

#include "../src/rdkafka_proto.h"


/**
 * @name Verify that the high-level consumer times out itself if
 *       heartbeats are not successful (issue #FIXME).
 */

static const char *commit_type;
static int rebalance_cnt = 0;

static void rebalance_cb (rd_kafka_t *rk,
                          rd_kafka_resp_err_t err,
                          rd_kafka_topic_partition_list_t *parts,
                          void *opaque) {

        rebalance_cnt++;
        TEST_SAY("Rebalance #%d: %s: %d partition(s)\n",
                 rebalance_cnt, rd_kafka_err2name(err), parts->cnt);

        if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
                test_consumer_assign("assign", rk, parts);
        } else {
                rd_kafka_resp_err_t commit_err;

                if (strcmp(commit_type, "auto")) {
                        TEST_SAY("Performing %s commit\n", commit_type);
                        commit_err = rd_kafka_commit(
                                rk, parts, !strcmp(commit_type, "async"));
                        TEST_ASSERT(commit_err == RD_KAFKA_RESP_ERR__STATE,
                                    "Expected %s commit to fail with "
                                    "ERR__STATE, not %s",
                                    rd_kafka_err2name(commit_err));
                }

                test_consumer_unassign("unassign", rk);
        }
}


/**
 * @brief Verify that session timeouts are handled by the consumer itself.
 *
 * @param use_commit_type "auto", "sync" (manual), "async" (manual)
 */
static void do_test_session_timeout (const char *use_commit_type) {
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *c;
        const char *groupid = "mygroup";
        const char *topic = "test";
        const int msgcnt = 1000;
        const size_t msgsize = 1000;

        commit_type = use_commit_type;

        TEST_SAY(_C_MAG "[ Test session timeout with %s commit ]\n",
                 commit_type);

        mcluster = test_mock_cluster_new(3, &bootstraps);

        rd_kafka_mock_coordinator_set(mcluster, "group", groupid, 1);

        /* Seed the topic with messages */
        test_produce_msgs_easy_v(topic, 0, 0, 0, msgcnt, msgsize,
                                 "bootstrap.servers", bootstraps,
                                 "batch.num.messages", "10",
                                 NULL);

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "group.id", groupid);
        test_conf_set(conf, "session.timeout.ms", "5000");
        test_conf_set(conf, "heartbeat.interval.ms", "1000");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit",
                      !strcmp(commit_type, "auto") ? "true" : "false");

        c = test_create_consumer(groupid, rebalance_cb, conf, NULL);

        test_consumer_subscribe(c, topic);

        /* Let Heartbeats fail after a couple of successful ones */
        rd_kafka_mock_push_request_errors(
                mcluster, RD_KAFKAP_Heartbeat,
                7,
                RD_KAFKA_RESP_ERR_NO_ERROR,
                RD_KAFKA_RESP_ERR_NO_ERROR,
                RD_KAFKA_RESP_ERR_NOT_COORDINATOR,
                RD_KAFKA_RESP_ERR_NOT_COORDINATOR,
                RD_KAFKA_RESP_ERR_NOT_COORDINATOR,
                RD_KAFKA_RESP_ERR_NOT_COORDINATOR,
                RD_KAFKA_RESP_ERR_NOT_COORDINATOR);


        TEST_SAY("Expecting assignment and revoke\n");
        test_consumer_poll("consume", c, 0, -1, 0, msgcnt, NULL);

        TEST_ASSERT(rebalance_cnt == 2, "expected assign + revoke, but got %d "
                    "rebalance events", rebalance_cnt);

        test_consumer_close(c);

        rd_kafka_destroy(c);

        test_mock_cluster_destroy(mcluster);

        TEST_SAY(_C_GRN "[ Test session timeout with %s commit PASSED ]\n",
                 commit_type);
}


int main_0106_cgrp_sess_timeout (int argc, char **argv) {

        do_test_session_timeout("auto");
        do_test_session_timeout("sync");
        do_test_session_timeout("async");

        return 0;
}
