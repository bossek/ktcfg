#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <librdkafka/rdkafka.h>

typedef struct {
    char *brokers;
    char *topic;
    char *key;
    char *value;
} Args;

static char *shift(int *argc, char ***argv) {
    assert(*argc > 0);
    char *result = **argv;
    *argv += 1;
    *argc -= 1;
    return result;
}

static int read_args(int *argc, char ***argv, Args *args) {
    while (*argc > 0) {
        const char *flag = shift(argc, argv);
        if (strcmp(flag, "-t") == 0) {
            if (*argc == 0) {
                fprintf(stderr, "[ERROR invalid args] Missing topic name.\n");
                return 1;
            }
            args->topic = shift(argc, argv);
        } else if (strcmp(flag, "-b") == 0) {
            if (*argc == 0) {
                fprintf(stderr, "[ERROR invalid args] Missing brokers config.\n");
                return 1;
            }
            args->brokers = shift(argc, argv);
        } else if (strcmp(flag, "-k") == 0) {
            if (*argc == 0) {
                fprintf(stderr, "[ERROR invalid args] Missing key name.\n");
                return 1;
            }
            args->key = shift(argc, argv);
        } else if (strcmp(flag, "-v") == 0) {
            if (*argc == 0) {
                fprintf(stderr, "[ERROR invalid args] Missing value.\n");
                return 1;
            }
            args->value = shift(argc, argv);
        } else {
            fprintf(stderr, "[ERROR invalid args] Unknown flag `%s`.\n", flag);
            return 1;
        }
    }

    if (!args->topic) {
        fprintf(stderr, "[ERROR invalid args] Topic is not specified.\n");
        return 1;
    }

    if (!args->key) {
        fprintf(stderr, "[ERROR invalid args] Key is not specified.\n");
        return 1;
    }

    if (!args->value) {
        fprintf(stderr, "[ERROR invalid args] Value is not specified.\n");
        return 1;
    }

    if (!args->brokers) {
        args->brokers = "localhost:9092";
    }

    return 0;
}

static void usage(FILE *stream, const char *program) {
    fprintf(stream, "Usage: %s -t <topic> -k <key> -v <value> [-b <brokers>]\n", program);
}

static void destroy_all(
        rd_kafka_ConfigResource_t *config,
        rd_kafka_t *rk,
        rd_kafka_AdminOptions_t *options,
        rd_kafka_queue_t *mainq,
        rd_kafka_event_t *rkev
        ) {
    if (rkev) { rd_kafka_event_destroy(rkev); }
    if (mainq) { rd_kafka_queue_destroy(mainq); }
    if (options) { rd_kafka_AdminOptions_destroy(options); }
    if (rk) { rd_kafka_destroy(rk); }
    if (config) { rd_kafka_ConfigResource_destroy(config); }
}

int main(int argc, char **argv) {
    const char *program = shift(&argc, &argv);
    Args args = { 0 };
    char errstr[512];

    rd_kafka_conf_t *conf;
    rd_kafka_ConfigResource_t *configs[1] = { NULL };
    rd_kafka_t *rk = NULL;
    rd_kafka_AdminOptions_t *options = NULL;
    rd_kafka_queue_t *mainq = NULL;
    rd_kafka_event_t *rkev = NULL;
    const rd_kafka_AlterConfigs_result_t *res;
    const rd_kafka_ConfigResource_t **rconfigs;
    size_t rconfig_cnt;

    if (read_args(&argc, &argv, &args)) {
        usage(stderr, program);
        return EXIT_FAILURE;
    }

    conf = rd_kafka_conf_new();

    if (rd_kafka_conf_set(
                conf, "bootstrap.servers", args.brokers,
                errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK
       ) {
        fprintf(stderr, "[ERROR config] %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return EXIT_FAILURE;
    }

    if (rd_kafka_conf_set(
                conf, "allow.auto.create.topics", "false",
                errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK
       ) {
        fprintf(stderr, "[ERROR config] %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return EXIT_FAILURE;
    }

    configs[0] = rd_kafka_ConfigResource_new(RD_KAFKA_RESOURCE_TOPIC, args.topic);
    if (rd_kafka_ConfigResource_set_config(
                configs[0], args.key, args.value) != RD_KAFKA_RESP_ERR_NO_ERROR
       ) {
        fprintf(stderr, "[ERROR set config] %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return EXIT_FAILURE;
    }

    rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        fprintf(stderr, "[ERROR client create] %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        destroy_all(configs[0], rk, options, mainq, rkev);
        return EXIT_FAILURE;
    }

    options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_ALTERCONFIGS);
    if (rd_kafka_AdminOptions_set_request_timeout(
                options, 5000,
                errstr, sizeof(errstr)) != RD_KAFKA_RESP_ERR_NO_ERROR
       ) {
        fprintf(stderr, "[ERROR set timeout] %s\n", errstr);
        destroy_all(configs[0], rk, options, mainq, rkev);
        return EXIT_FAILURE;
    }

    mainq = rd_kafka_queue_get_main(rk);
    rd_kafka_AlterConfigs(rk, configs, 1, options, mainq);
    rkev = rd_kafka_queue_poll(mainq, 10000);

    if (!rkev) {
        fprintf(stderr, "[ERROR timed out]\n");
        destroy_all(configs[0], rk, options, mainq, rkev);
        return EXIT_FAILURE;
    }
    if (rd_kafka_event_error(rkev) != RD_KAFKA_RESP_ERR_NO_ERROR) {
        fprintf(stderr, "[ERROR event] (%d) %s\n",
                rd_kafka_event_error(rkev),
                rd_kafka_event_error_string(rkev));
        destroy_all(configs[0], rk, options, mainq, rkev);
        return EXIT_FAILURE;
    }
    if (rd_kafka_event_type(rkev) == RD_KAFKA_EVENT_ERROR) {
        fprintf(stderr, "[ERROR event] (%d) %s\n",
                rd_kafka_event_type(rkev),
                rd_kafka_event_error_string(rkev));
        destroy_all(configs[0], rk, options, mainq, rkev);
        return EXIT_FAILURE;
    }
    if (rd_kafka_event_type(rkev) != RD_KAFKA_EVENT_ALTERCONFIGS_RESULT) {
        fprintf(stderr, "[ERROR unxpected event type] (%d) %s\n",
                rd_kafka_event_type(rkev),
                rd_kafka_event_name(rkev));
        destroy_all(configs[0], rk, options, mainq, rkev);
        return EXIT_FAILURE;
    }

    res = rd_kafka_event_AlterConfigs_result(rkev);
    rconfigs = rd_kafka_AlterConfigs_result_resources(res, &rconfig_cnt);
    assert(rconfig_cnt == 1);

    if (rd_kafka_ConfigResource_error(rconfigs[0]) != RD_KAFKA_RESP_ERR_NO_ERROR) {
        fprintf(stderr, "[ERROR config resource] (%d) %s\n",
                rd_kafka_ConfigResource_error(rconfigs[0]),
                rd_kafka_ConfigResource_error_string(rconfigs[0]));
        destroy_all(configs[0], rk, options, mainq, rkev);
        return EXIT_FAILURE;
    }

    destroy_all(configs[0], rk, options, mainq, rkev);
    return EXIT_SUCCESS;
}
