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
    int frequency;
} WordFreq;

typedef struct {
    void *context;
    char *port;
    int *running;
} WorkerArgs;

// Wandelt String in lowerCase um
void to_lowercase(char *str) {
    for (int i = 0; str[i]; i++) {
        str[i] = tolower((unsigned char)str[i]);
    }
}

// schaut Zeichen = Worttrenner
int is_separator(char c) {
    return !isalpha((unsigned char)c);
}

// MAP-Funktion
char *map_function(const char *text) {
    WordFreq *results = malloc(sizeof(WordFreq) * MAX_WORDS);
    if (!results) return strdup("");

    int result_count = 0;
    int len = strlen(text);
    int i = 0;

    while (i < len) {
        // Nicht-alphabetische Zeichen überspringen
        while (i < len && !isalpha((unsigned char)text[i])) {
            i++;
        }

        if (i >= len) break;

        // Wort extrahieren und direkt in LowerCase umwandeln
        char word[MAX_WORD_LEN];
        int wi = 0;
        while (i < len && isalpha((unsigned char)text[i]) && wi < MAX_WORD_LEN - 1) {
            word[wi++] = tolower((unsigned char)text[i]);
            i++;
        }
        word[wi] = '\0';

        if (wi == 0) continue;

        // ist Wort in ergebniss
        int found = -1;
        for (int j = 0; j < result_count; j++) {
            if (strcmp(results[j].word, word) == 0) {
                found = j;
                break;
            }
        }

        if (found >= 0) {
            results[found].frequency++;
        } else {
            // Neues Wort hinzufügen
            if (result_count < MAX_WORDS) {
                strcpy(results[result_count].word, word);
                results[result_count].frequency = 1;
                result_count++;
            }
        }
    }

    // Ausgabe aufbauen
    char *output = malloc(MAX_MSG_LEN);
    if (!output) {
        free(results);
        return strdup("");
    }

    char *out_ptr = output;
    size_t remaining = MAX_MSG_LEN - 1;

    for (int i = 0; i < result_count; i++) {
        size_t word_len = strlen(results[i].word);
        int freq = results[i].frequency;

        // Prüfen, ob genug Platz für Wort + Frequenz vorhanden ist
        if (word_len + freq + 1 >= remaining) {
            break;
        }

        // Wort kopieren
        memcpy(out_ptr, results[i].word, word_len);
        out_ptr += word_len;
        remaining -= word_len;

        // Frequenz anhängen
        for (int j = 0; j < freq; j++) {
            *out_ptr++ = '1';
            remaining--;
        }
    }
    *out_ptr = '\0';

    free(results);
    return output;
}

// REDUCE-Funktion
char *reduce_function(const char *input) {
    WordFreq *results = malloc(sizeof(WordFreq) * MAX_WORDS);
    if (!results) return strdup("");

    int result_count = 0;
    const char *ptr = input;
    int input_len = strlen(input);

    while (*ptr && (ptr - input) < input_len) {
        // Nicht-alphabetische Zeichen überspringen
        while (*ptr && !isalpha((unsigned char)*ptr)) {
            ptr++;
        }

        if (!*ptr) break;

        // Wort extrahieren
        char word[MAX_WORD_LEN];
        int wi = 0;
        while (*ptr && isalpha((unsigned char)*ptr) && wi < MAX_WORD_LEN - 1) {
            word[wi++] = *ptr++;
        }
        word[wi] = '\0';

        if (wi == 0) continue;

        // Anzahl der '1' zählen
        int freq = 0;
        while (*ptr == '1') {
            freq++;
            ptr++;
        }

        // Falls keine Frequenz gefunden wurde, auf 1 setzen
        if (freq == 0) freq = 1;

        // Wort suchen oder neu anlegen
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

    // Ausgabe aufbauen
    char *output = malloc(MAX_MSG_LEN);
    if (!output) {
        free(results);
        return strdup("");
    }

    char *out_ptr = output;
    size_t remaining = MAX_MSG_LEN - 1;

    for (int i = 0; i < result_count; i++) {
        char temp[MAX_WORD_LEN + 32];
        int temp_len = snprintf(temp, sizeof(temp), "%s%d", results[i].word, results[i].frequency);

        if ((size_t)temp_len >= remaining) {
            break;
        }

        memcpy(out_ptr, temp, temp_len);
        out_ptr += temp_len;
        remaining -= temp_len;
    }
    *out_ptr = '\0';

    free(results);
    return output;
}

static void send_response_and_free(void *socket, char *response) {
    if (!response) {
        return;
    }
    size_t len = strnlen(response, MAX_MSG_LEN - 1);
    response[len] = '\0';
    zmq_send(socket, response, len + 1, 0);
    free(response);
}

// Thread-Funktion für Worker
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

        // Empfangstimeout setzen
        int timeout = 100;
        zmq_setsockopt(socket, ZMQ_RCVTIMEO, &timeout, sizeof(timeout));

        int size = zmq_recv(socket, buffer, MAX_MSG_LEN - 1, 0);

        if (size < 0) {
            continue;
        }

        buffer[size] = '\0';

        char *response = NULL;

        // Nachrichtentyp auswerten
        if (strncmp(buffer, "map", 3) == 0) {
            // MAP-Befehl
            char *payload = buffer + 3;
            response = map_function(payload);
        } else if (strncmp(buffer, "red", 3) == 0) {
            // REDUCE-Befehl
            char *payload = buffer + 3;
            response = reduce_function(payload);
        } else if (strncmp(buffer, "rip", 3) == 0) {
            // sauber beenden
            response = strdup("rip");
            send_response_and_free(socket, response);
            *args->running = 0;
            break;
        } else {
            // Unbekannter Befehl
            response = strdup("");
        }

        send_response_and_free(socket, response);
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

    if (!threads || !args || !running_flags) {
        fprintf(stderr, "Memory allocation failed\n");
        free(threads);
        free(args);
        free(running_flags);
        zmq_ctx_destroy(context);
        return 1;
    }

    // Worker-Threads starten
    for (int i = 0; i < num_ports; i++) {
        running_flags[i] = 1;
        args[i].context = context;
        args[i].port = ports[i];
        args[i].running = &running_flags[i];

        pthread_create(&threads[i], NULL, worker_thread, &args[i]);
    }

    // Auf alle Threads warten
    for (int i = 0; i < num_ports; i++) {
        pthread_join(threads[i], NULL);
    }

    free(threads);
    free(args);
    free(running_flags);

    zmq_ctx_destroy(context);

    return 0;
}
