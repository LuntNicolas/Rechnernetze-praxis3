#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <pthread.h>
#include <zmq.h>

#define MAX_MSG_LEN 1500
#define MAX_WORD_LEN 256
#define MAX_WORDS 500000

typedef struct {
    char word[MAX_WORD_LEN];
    int frequency;
} WordCount;

WordCount *global_words = NULL;
int global_word_count = 0;
pthread_mutex_t word_mutex = PTHREAD_MUTEX_INITIALIZER;

void to_lowercase(char *str) {
    for (int i = 0; str[i]; i++) {
        str[i] = tolower((unsigned char)str[i]);
    }
}

int is_separator(char c) {
    return !isalpha((unsigned char)c);
}

void add_word(const char *word, int freq) {
    pthread_mutex_lock(&word_mutex);

    for (int i = 0; i < global_word_count; i++) {
        if (strcmp(global_words[i].word, word) == 0) {
            global_words[i].frequency += freq;
            pthread_mutex_unlock(&word_mutex);
            return;
        }
    }

    if (global_word_count < MAX_WORDS) {
        strcpy(global_words[global_word_count].word, word);
        global_words[global_word_count].frequency = freq;
        global_word_count++;
    }

    pthread_mutex_unlock(&word_mutex);
}

int compare_words(const void *a, const void *b) {
    WordCount *wa = (WordCount *)a;
    WordCount *wb = (WordCount *)b;

    if (wb->frequency != wa->frequency) {
        return wb->frequency - wa->frequency;
    }

    return strcmp(wa->word, wb->word);
}

char *read_file(const char *filename) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        return NULL;
    }

    fseek(file, 0, SEEK_END);
    long size = ftell(file);
    fseek(file, 0, SEEK_SET);

    char *content = malloc(size + 1);
    fread(content, 1, size, file);
    content[size] = '\0';

    fclose(file);
    return content;
}

char *send_request(void *context, const char *port, const char *message) {
    void *socket = zmq_socket(context, ZMQ_REQ);
    if (!socket) {
        return strdup("");
    }

    char endpoint[64];
    snprintf(endpoint, sizeof(endpoint), "tcp://localhost:%s", port);

    if (zmq_connect(socket, endpoint) != 0) {
        zmq_close(socket);
        return strdup("");
    }

    zmq_send(socket, message, strlen(message) + 1, 0);

    char buffer[MAX_MSG_LEN];
    memset(buffer, 0, MAX_MSG_LEN);
    int size = zmq_recv(socket, buffer, MAX_MSG_LEN - 1, 0);

    char *response = (size > 0) ? strdup(buffer) : strdup("");

    zmq_close(socket);
    return response;
}

void parse_response(const char *input) {
    if (!input || strlen(input) == 0) return;

    const char *ptr = input;

    while (*ptr) {
        // Skip non-alpha characters
        while (*ptr && !isalpha((unsigned char)*ptr)) ptr++;
        if (!*ptr) break;

        // Extract word
        char word[MAX_WORD_LEN] = {0};
        int wi = 0;
        while (*ptr && isalpha((unsigned char)*ptr) && wi < MAX_WORD_LEN - 1) {
            word[wi++] = *ptr++;
        }
        word[wi] = '\0';

        if (wi == 0) continue;

        // Extract frequency (numeric only)
        int freq = 0;
        while (*ptr && isdigit((unsigned char)*ptr)) {
            freq = freq * 10 + (*ptr - '0');
            ptr++;
        }

        if (freq > 0) {
            add_word(word, freq);
        }
    }
}

// FIXED: Improved chunking that properly handles word boundaries
int get_chunk_size(const char *text, int pos, int max_size, int text_len) {
    if (pos >= text_len) {
        return 0;
    }

    if (pos + max_size >= text_len) {
        return text_len - pos;
    }

    int chunk_size = max_size;

    // Move back to find a separator (space, punctuation, etc.)
    while (chunk_size > 0 && !is_separator(text[pos + chunk_size])) {
        chunk_size--;
    }

    // If we backed up too much, try moving forward instead
    if (chunk_size < max_size / 4) {
        chunk_size = max_size;
        while (pos + chunk_size < text_len && !is_separator(text[pos + chunk_size])) {
            chunk_size++;
        }
    }

    // Include the separator in this chunk
    if (pos + chunk_size < text_len && is_separator(text[pos + chunk_size])) {
        chunk_size++;
    }

    return chunk_size;
}

// For reduce input (word + ones), split only between tokens to avoid cutting counts
int get_reduce_chunk_size(const char *text, int pos, int max_size, int text_len) {
    if (pos >= text_len) {
        return 0;
    }

    int end = pos + max_size;
    if (end >= text_len) {
        return text_len - pos;
    }

    // Look for a boundary: digit followed by alpha
    for (int i = end; i > pos; i--) {
        if (isalpha((unsigned char)text[i]) && isdigit((unsigned char)text[i - 1])) {
            return i - pos;
        }
    }

    // Fallback: no safe boundary found, return max_size
    return max_size;
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <file.txt> <worker port 1> ... <worker port n>\n", argv[0]);
        return 1;
    }

    const char *filename = argv[1];
    int num_workers = argc - 2;
    char **worker_ports = &argv[2];

    global_words = malloc(sizeof(WordCount) * MAX_WORDS);
    if (!global_words) {
        fprintf(stderr, "Memory allocation failed\n");
        return 1;
    }

    char *text = read_file(filename);
    if (!text) {
        free(global_words);
        return 1;
    }

    void *context = zmq_ctx_new();

    int text_len = strlen(text);
    int max_payload = MAX_MSG_LEN - 50; // More conservative safety margin

    // MAP PHASE - send chunks
    int chunk_idx = 0;
    int pos = 0;

    char **map_results = malloc(sizeof(char *) * 10000);
    if (!map_results) {
        fprintf(stderr, "Memory allocation failed\n");
        free(text);
        free(global_words);
        zmq_ctx_destroy(context);
        return 1;
    }
    int map_result_count = 0;

    while (pos < text_len) {
        int chunk_size = get_chunk_size(text, pos, max_payload, text_len);
        if (chunk_size == 0) break;

        char *message = malloc(MAX_MSG_LEN);
        if (!message) {
            fprintf(stderr, "Memory allocation failed\n");
            break;
        }

        strcpy(message, "map");
        memcpy(message + 3, text + pos, chunk_size);
        message[3 + chunk_size] = '\0';

        char *response = send_request(context, worker_ports[chunk_idx % num_workers], message);
        if (map_result_count < 10000) {
            map_results[map_result_count++] = response;
        } else {
            free(response);
        }

        free(message);
        pos += chunk_size;
        chunk_idx++;
    }

    // Combine map results
    size_t total_len = 0;
    for (int i = 0; i < map_result_count; i++) {
        if (map_results[i]) {
            total_len += strlen(map_results[i]);
        }
    }

    char *combined = malloc(total_len + 100);
    if (!combined) {
        fprintf(stderr, "Memory allocation failed\n");
        for (int i = 0; i < map_result_count; i++) {
            free(map_results[i]);
        }
        free(map_results);
        free(text);
        free(global_words);
        zmq_ctx_destroy(context);
        return 1;
    }

    char *pos_ptr = combined;
    for (int i = 0; i < map_result_count; i++) {
        if (map_results[i]) {
            size_t len = strlen(map_results[i]);
            memcpy(pos_ptr, map_results[i], len);
            pos_ptr += len;
            free(map_results[i]);
        }
    }
    *pos_ptr = '\0';
    free(map_results);

    // REDUCE PHASE
    int combined_len = strlen(combined);
    pos = 0;
    chunk_idx = 0;

    while (pos < combined_len) {
        int chunk_size = get_reduce_chunk_size(combined, pos, max_payload, combined_len);
        if (chunk_size == 0) break;

        char *message = malloc(MAX_MSG_LEN);
        if (!message) {
            fprintf(stderr, "Memory allocation failed\n");
            break;
        }

        strcpy(message, "red");
        memcpy(message + 3, combined + pos, chunk_size);
        message[3 + chunk_size] = '\0';

        char *response = send_request(context, worker_ports[chunk_idx % num_workers], message);
        parse_response(response);
        free(response);

        free(message);
        pos += chunk_size;
        chunk_idx++;
    }

    free(combined);

    // COMBINE and OUTPUT
    qsort(global_words, global_word_count, sizeof(WordCount), compare_words);

    printf("word,frequency\n");
    for (int i = 0; i < global_word_count; i++) {
        printf("%s,%d\n", global_words[i].word, global_words[i].frequency);
    }

    // RIP - shutdown workers
    for (int i = 0; i < num_workers; i++) {
        char *response = send_request(context, worker_ports[i], "rip");
        free(response);
    }

    free(text);
    free(global_words);
    zmq_ctx_destroy(context);

    return 0;
}
