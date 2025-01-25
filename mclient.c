#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <libmemcached/memcached.h>
#include <math.h>
#include <unistd.h>

// Around 4 GiB.
#define NUM_KEYS 4194304
#define MAX_KEY_SIZE 32
#define VAL_SIZE 1024
#define NUM_THREADS 32
#define ZIPF_SKEW 0.99

struct zipf_data {
    char buf[VAL_SIZE];
};

struct zipf_data *zipf_data;
// Zipfian distribution state
double *zipf_cdf;

// Argv
const char *server;
int port;
int total_iterations;

static void generate_zipf_cdf(double skew, int n) {
    // zipf_cdf = (double *)malloc(n * sizeof(double));
    // double sum = 0.0;
    // for (int i = 1; i <= n; i++) {
    //     sum += 1.0 / pow(i, skew);
    // }
    // double cumulative_sum = 0.0;
    // for (int i = 0; i < n; i++) {
    //     cumulative_sum += 1.0 / pow(i + 1, skew);
    //     zipf_cdf[i] = cumulative_sum / sum;
    // }
}

static void generate_zipf_data(void) {
    size_t data_size_bytes = NUM_KEYS * sizeof(*zipf_data);

    zipf_data = malloc(data_size_bytes);

    int fd = open("/dev/urandom", O_RDONLY);
    read(fd, zipf_data, data_size_bytes);
    close(fd);
}

static int zipf_sample(int n) {
    // uniform random for now.
    return rand() % n;

    // double r = (double)rand() / RAND_MAX;
    //
    // int left = 0;
    // int right = NUM_KEYS - 1;
    // int highest_below_r = 0;  // Store the closest value strictly below the key
    //
    // while (left <= right) {
    //     int mid = (right + left) / 2;
    //     // printf("%d, %d, %d\n", left, mid, right);
    //
    //     if (zipf_cdf[mid] < r) {
    //         highest_below_r = mid;  // Update best candidate
    //         left = mid + 1;  // Search the right half
    //     } else {
    //         right = mid - 1;  // Search the left half
    //     }
    // }
    //
    // return highest_below_r;
}

typedef struct {
    int id;
} thread_args_t;

void *memcached_load_generator(void *arg) {
    thread_args_t *args = (thread_args_t *)arg;
    char key[MAX_KEY_SIZE];
    int thread_iterations;

    // Initialize memcached client
    memcached_st *memc;
    memcached_server_st *servers;
    memcached_return rc;

    thread_iterations = total_iterations / NUM_THREADS;
    if (args->id == 0)
        thread_iterations += total_iterations % NUM_THREADS;

    memc = memcached_create(NULL);
    servers = memcached_server_list_append(NULL, server, port, &rc);
    memcached_server_push(memc, servers);

    for (int i = 0; i < thread_iterations; i++) {
        int key_index = zipf_sample(NUM_KEYS);
        snprintf(key, MAX_KEY_SIZE, "key_%d", key_index);
        // printf("%s\n", key);

        if (i % 2 == 0) {  // 50% `get`
            memcached_return rc;
            size_t value_length;
            uint32_t flags;
            char *retrieved_value = memcached_get(memc, key, strlen(key), &value_length, &flags, &rc);
            if (rc == MEMCACHED_SUCCESS) {
                free(retrieved_value);
            }
        } else {  // 50% `set`
            memcached_return rc;

            rc = memcached_set(memc, key, strlen(key), zipf_data[key_index].buf, VAL_SIZE,
                    (time_t)0, (uint32_t)0);
            if (rc != MEMCACHED_SUCCESS) {
                printf("ERROR: %s\n", memcached_strerror(memc, rc));
                exit(1);
            }
        }
    }

    memcached_free(memc);
    memcached_server_list_free(servers);
    pthread_exit(NULL);
}

int main(int argc, char **argv) {
    if (argc != 4) {
        fprintf(stderr, "Usage: %s <server> <port> <total_iterations>\n", argv[0]);
        return EXIT_FAILURE;
    }

    server = argv[1];
    port = atoi(argv[2]);
    total_iterations = atoi(argv[3]);

    // Prepare Zipfian CDF for key distribution
    printf("Init\n");
    generate_zipf_data();

    // Create threads
    pthread_t threads[NUM_THREADS];
    thread_args_t thread_args[NUM_THREADS];

    for (int i = 0; i < NUM_THREADS; i++) {
        thread_args[i].id = i;
        pthread_create(&threads[i], NULL, memcached_load_generator, (void *)&thread_args[i]);
    }

    // Wait for threads to finish
    printf("Waiting for benchmark finish\n");
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    // Cleanup
    free(zipf_cdf);
    // free(zipf_data);

    return EXIT_SUCCESS;
}
