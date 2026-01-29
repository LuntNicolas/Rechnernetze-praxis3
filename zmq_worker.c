#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <pthread.h>
#include <zmq.h>

#define MAX_MSG_LEN 1500
#define MAX_WORD_LEN 256
#define MAX_WORDS 50000

typedef struct {
    char word[MAX_WORD_LEN];
    char frequency[MAX_MSG_LEN];
} MapResult;

typedef struct {
    char word[MAX_WORD_LEN];
    int frequency;
} ReduceResult;

typedef struct {
    void *context;
    char *port;
    int *running;
} WorkerArgs;

// Convert string to lowercase
void to_lowercase(char *str) {
    for (int i = 0; str[i]; i++) {
        str[i] = tolower((unsigned char)str[i]);
    }
}

// Check if character is a word separator
int is_separator(char c) {
    return !isalpha((unsigned char)c);
}

// MAP function - Fixed to replace non-alpha with '1'
char *map_function(const char *text) {
    // First, normalize the text: replace non-alpha with '1', convert to lowercase
    int len = strlen(text);
    char *normalized = malloc(len + 1);
    int ni = 0;

    for (int i = 0; i < len; i++) {
        if (isalpha((unsigned char)text[i])) {
            normalized[ni++] = tolower((unsigned char)text[i]);
        } else {
            normalized[ni++] = '1';
        }
    }
    normalized[ni] = '\0';

    // Now process the normalized text
    MapResult *results = malloc(sizeof(MapResult) * MAX_WORDS);
    int result_count = 0;

    int i = 0;
    while (i < ni) {
        // Skip '1's
        while (i < ni && normalized[i] == '1') {
            i++;
        }

        if (i >= ni) break;

        // Extract word
        char word[MAX_WORD_LEN];
        int wi = 0;
        while (i < ni && normalized[i] != '1' && wi < MAX_WORD_LEN - 1) {
            word[wi++] = normalized[i++];
        }
        word[wi] = '\0';

        if (wi == 0) continue;

        // Check if word already exists in results
        int found = -1;
        for (int j = 0; j < result_count; j++) {
            if (strcmp(results[j].word, word) == 0) {
                found = j;
                break;
            }
        }

        if (found >= 0) {
            // Add another '1' to frequency
            int freq_len = strlen(results[found].frequency);
            if (freq_len < MAX_MSG_LEN - 1) {
                results[found].frequency[freq_len] = '1';
                results[found].frequency[freq_len + 1] = '\0';
            }
        } else {
            // Add new word
            if (result_count < MAX_WORDS) {
                strcpy(results[result_count].word, word);
                strcpy(results[result_count].frequency, "1");
                result_count++;
            }
        }
    }

    free(normalized);

    // Build output string
    char *output = malloc(MAX_MSG_LEN);
    output[0] = '\0';
    size_t current_len = 0;

    for (int i = 0; i < result_count; i++) {
        size_t word_len = strlen(results[i].word);
        size_t freq_len = strlen(results[i].frequency);

        if (current_len + word_len + freq_len + 1 < MAX_MSG_LEN) {
            strcat(output, results[i].word);
            strcat(output, results[i].frequency);
            current_len += word_len + freq_len;
        } else {
            break;
        }
    }

    free(results);
    return output;
}

// REDUCE function - Fixed parsing logic
char *reduce_function(const char *input) {
    ReduceResult *results = malloc(sizeof(ReduceResult) * MAX_WORDS);
    int result_count = 0;

    const char *ptr = input;

    while (*ptr) {
        // Skip non-alphabetic characters
        while (*ptr && !isalpha((unsigned char)*ptr)) {
            ptr++;
        }

        if (!*ptr) break;

        // Extract word
        char word[MAX_WORD_LEN];
        int wi = 0;
        while (*ptr && isalpha((unsigned char)*ptr) && wi < MAX_WORD_LEN - 1) {
            word[wi++] = *ptr++;
        }
        word[wi] = '\0';

        if (wi == 0) continue;

        // Extract frequency
        int freq = 0;

        // Check if it's a sequence of '1's or a multi-digit number
        if (*ptr == '1') {
            // Count consecutive '1's
            while (*ptr == '1') {
                freq++;
                ptr++;
            }
        } else if (isdigit((unsigned char)*ptr)) {
            // Parse as a regular number
            while (*ptr && isdigit((unsigned char)*ptr)) {
                freq = freq * 10 + (*ptr - '0');
                ptr++;
            }
        }

        if (freq == 0) freq = 1;

        // Find or add word
        int found = -1;
        for (int i = 0; i < result_count; i++) {
            if (strcmp(results[i].word, word) == 0) {
                found = i;
                break;
            }
        }

        if (found >= 0) {
            results[found].frequency += freq;
        } else {
            if (result_count < MAX_WORDS) {
                strcpy(results[result_count].word, word);
                results[result_count].frequency = freq;
                result_count++;
            }
        }
    }

    // Build output
    char *output = malloc(MAX_MSG_LEN);
    output[0] = '\0';
    size_t current_len = 0;

    for (int i = 0; i < result_count; i++) {
        char temp[MAX_WORD_LEN + 32];
        int temp_len = snprintf(temp, sizeof(temp), "%s%d", results[i].word, results[i].frequency);

        if (current_len + temp_len + 1 < MAX_MSG_LEN) {
            strcat(output, temp);
            current_len += temp_len;
        } else {
            break;
        }
    }

    free(results);
    return output;
}

// Worker thread function
void *worker_thread(void *arg) {
    WorkerArgs *args = (WorkerArgs *)arg;

    void *socket = zmq_socket(args->context, ZMQ_REP);
    if (!socket) {
        fprintf(stderr, "Error creating socket\n");
        return NULL;
    }

    char endpoint[64];
    snprintf(endpoint, sizeof(endpoint), "tcp://*:%s", args->port);

    if (zmq_bind(socket, endpoint) != 0) {
        fprintf(stderr, "Error binding to %s\n", endpoint);
        zmq_close(socket);
        return NULL;
    }

    while (*args->running) {
        char buffer[MAX_MSG_LEN];
        memset(buffer, 0, MAX_MSG_LEN);

        // Set receive timeout
        int timeout = 100;
        zmq_setsockopt(socket, ZMQ_RCVTIMEO, &timeout, sizeof(timeout));

        int size = zmq_recv(socket, buffer, MAX_MSG_LEN - 1, 0);

        if (size < 0) {
            continue;
        }

        buffer[size] = '\0';

        char *response = NULL;

        // Parse message type
        if (strncmp(buffer, "map", 3) == 0) {
            // MAP command
            char *payload = buffer + 3;
            response = map_function(payload);
        } else if (strncmp(buffer, "red", 3) == 0) {
            // REDUCE command
            char *payload = buffer + 3;
            response = reduce_function(payload);
        } else if (strncmp(buffer, "rip", 3) == 0) {
            // RIP command
            response = strdup("rip");
            zmq_send(socket, response, strlen(response) + 1, 0);
            free(response);
            *args->running = 0;
            break;
        } else {
            // Unknown command
            response = strdup("");
        }

        if (response) {
            zmq_send(socket, response, strlen(response) + 1, 0);
            free(response);
        }
    }

    zmq_close(socket);
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <worker port 1> <worker port 2> ... <worker port n>\n", argv[0]);
        return 1;
    }

    int num_ports = argc - 1;
    char **ports = &argv[1];

    void *context = zmq_ctx_new();

    pthread_t *threads = malloc(sizeof(pthread_t) * num_ports);
    WorkerArgs *args = malloc(sizeof(WorkerArgs) * num_ports);
    int *running_flags = malloc(sizeof(int) * num_ports);

    // Start worker threads
    for (int i = 0; i < num_ports; i++) {
        running_flags[i] = 1;
        args[i].context = context;
        args[i].port = ports[i];
        args[i].running = &running_flags[i];

        pthread_create(&threads[i], NULL, worker_thread, &args[i]);
    }

    // Wait for all threads
    for (int i = 0; i < num_ports; i++) {
        pthread_join(threads[i], NULL);
    }

    free(threads);
    free(args);
    free(running_flags);

    zmq_ctx_destroy(context);

    return 0;
}