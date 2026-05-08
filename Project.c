#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include<unistd.h>
#include<time.h>

#define PRODUCERS 5
#define CONSUMERS 5

int *buffer; //dynamic
int  capacity;
int  head  = 0;
int  tail  = 0;
int  count = 0;

volatile int running=1;

pthread_mutex_t prod_fair_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t prod_fair_cond = PTHREAD_COND_INITIALIZER;
int prod_turn = 0;

pthread_mutex_t cons_fair_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cons_fair_cond = PTHREAD_COND_INITIALIZER;
int cons_turn = 0;

int produced_stats[PRODUCERS];
int consumed_stats[CONSUMERS];

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;
sem_t empty;
sem_t full;

void log_event(const char *msg) { //this is just print X
	pthread_mutex_lock(&log_mutex);
	printf("%s\n", msg);
	pthread_mutex_unlock(&log_mutex);
}

int slot_used(int idx) {
	if (count == 0) return 0;
	if (count == capacity) return 1;
	if (head < tail) return (idx >= head && idx < tail);
	return (idx >= head || idx < tail);
}

void log_buffer_state_locked(void) {
	char line[512];
	int pos = 0;

	pos += snprintf(line + pos, sizeof(line) - pos, "State: [");
	for (int i = 0; i < capacity; i++) {
		if (slot_used(i)) {
			pos += snprintf(line + pos, sizeof(line) - pos, "%2d", buffer[i]);
		} else {
			pos += snprintf(line + pos, sizeof(line) - pos, " _");
		}
		if (i != capacity - 1) {
			pos += snprintf(line + pos, sizeof(line) - pos, " ");
		}
	}
	pos += snprintf(line + pos, sizeof(line) - pos, "] H=%d T=%d", head, tail);
	log_event(line);
}

void request_shutdown(void) {
	running = 0;

	for (int i = 0; i < PRODUCERS; i++) {
		sem_post(&empty);
	}
	for (int i = 0; i < CONSUMERS; i++) {
		sem_post(&full);
	}

	pthread_mutex_lock(&prod_fair_mutex);
	pthread_cond_broadcast(&prod_fair_cond);
	pthread_mutex_unlock(&prod_fair_mutex);

	pthread_mutex_lock(&cons_fair_mutex);
	pthread_cond_broadcast(&cons_fair_cond);
	pthread_mutex_unlock(&cons_fair_mutex);
}

void produce(int item, int id) { //writes on tail
	sem_wait(&empty);
	pthread_mutex_lock(&mutex);

	if (!running) {
		pthread_mutex_unlock(&mutex);
		sem_post(&empty);
		return;
	}

	buffer[tail] = item;
	tail = (tail + 1) % capacity;
	count++;

	produced_stats[id]++;

	char msg[64];
	snprintf(msg, sizeof(msg), "Producer %d produced: %d | Buffer: %d/%d", id+1, item, count, capacity);
	log_event(msg);
	log_buffer_state_locked();

	pthread_mutex_unlock(&mutex);
	sem_post(&full);
}

int consume(int id) { //reads from head
	sem_wait(&full);
	pthread_mutex_lock(&mutex);

	if (!running || count == 0) {
		pthread_mutex_unlock(&mutex);
		sem_post(&full);
		return -1;
	}

	int item = buffer[head];
	head = (head + 1) % capacity;
	count--;

	consumed_stats[id]++;

	char msg[64];
	snprintf(msg, sizeof(msg), "Consumer %d consumed: %d | Buffer: %d/%d",id+1, item, count, capacity);
	log_event(msg);
	log_buffer_state_locked();

	pthread_mutex_unlock(&mutex);
	sem_post(&empty);

	return item;
}

void* producer_thread(void* arg){
	int id=*(int*)arg;

	while(running){
		pthread_mutex_lock(&prod_fair_mutex);
		while (running && id != prod_turn) {
			pthread_cond_wait(&prod_fair_cond, &prod_fair_mutex);
		}
		if (!running) {
			pthread_mutex_unlock(&prod_fair_mutex);
			break;
		}
		pthread_mutex_unlock(&prod_fair_mutex);

		int item = rand() % 100;
		produce(item, id);

		pthread_mutex_lock(&prod_fair_mutex);
		prod_turn = (prod_turn + 1) % PRODUCERS;
		pthread_cond_broadcast(&prod_fair_cond);
		pthread_mutex_unlock(&prod_fair_mutex);

		sleep(rand() %3 +1 ); //rate
	}
	pthread_exit(NULL);
}

void* consumer_thread(void*arg){
	int id=*(int*)arg;

	while(running){
		pthread_mutex_lock(&cons_fair_mutex);
		while (running && id != cons_turn) {
			pthread_cond_wait(&cons_fair_cond, &cons_fair_mutex);
		}
		if (!running) {
			pthread_mutex_unlock(&cons_fair_mutex);
			break;
		}
		pthread_mutex_unlock(&cons_fair_mutex);

		consume(id);

		pthread_mutex_lock(&cons_fair_mutex);
		cons_turn = (cons_turn + 1) % CONSUMERS;
		pthread_cond_broadcast(&cons_fair_cond);
		pthread_mutex_unlock(&cons_fair_mutex);
		sleep(rand()%4+1);
	}

	pthread_exit(NULL);
}

int main(){

	printf("Enter buffer size: ");
	scanf("%d", &capacity);

	buffer = (int *)malloc(capacity * sizeof(int));

	sem_init(&empty, 0, capacity);
    sem_init(&full, 0, 0);
	
	pthread_t producers[PRODUCERS];
	pthread_t consumers[CONSUMERS];

	int pid[PRODUCERS];
	int cid[CONSUMERS];

	for(int i=0; i<PRODUCERS; i++){
		pid[i]=i;
		pthread_create(&producers[i], NULL, producer_thread, &pid[i]);
	}

	for(int i=0; i<CONSUMERS; i++){
		cid[i]=i;
		pthread_create(&consumers[i], NULL, consumer_thread, &cid[i]);
	}

	sleep(20);
	request_shutdown(); //while loops stop

	for (int i = 0; i < PRODUCERS; i++){
		pthread_join(producers[i], NULL);
	}
        
    for (int i = 0; i < CONSUMERS; i++){
		pthread_join(consumers[i], NULL);
	}

	printf("THREAD STATS: ");

	for (int i = 0; i < PRODUCERS; i++){
        printf("Producer %d produced %d items\n", i + 1, produced_stats[i]);
	}

	for (int i = 0; i < CONSUMERS; i++){
        printf("Consumer %d consumed %d items\n", i + 1, consumed_stats[i]);
	}

	free(buffer);

	sem_destroy(&empty);
	sem_destroy(&full);

	pthread_mutex_destroy(&mutex);
	pthread_mutex_destroy(&log_mutex);
	pthread_mutex_destroy(&prod_fair_mutex);
	pthread_mutex_destroy(&cons_fair_mutex);
	pthread_cond_destroy(&prod_fair_cond);
	pthread_cond_destroy(&cons_fair_cond);

	return 0;
}
