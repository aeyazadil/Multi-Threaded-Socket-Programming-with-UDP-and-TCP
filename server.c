/* Server: multithreaded with request queue (FCFS or RR).
   Build: gcc -pthread server.c -o server */

#include "socketUtils.h"
#include <pthread.h>
#include <signal.h>
#include <errno.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#define WORKER_COUNT 4
#define RR_QUANTUM 2    // RR timeslice units
#define SERVICE_UNITS 6 // Simulated work units per request

typedef enum
{
    REQ_NEW = 0,
    REQ_PROCESSING,
    REQ_READY_FOR_UDP
} ReqState;

typedef struct Request
{
    int client_fd;
    struct sockaddr_in client_addr;
    ReqState state;
    int units_remaining;
    int sock_udp_listen; // UDP socket for this request
    int udp_port;
    struct Request *next;
} Request;

// Request queue
typedef struct
{
    Request *head;
    Request *tail;
    pthread_mutex_t lock;
    pthread_cond_t cond;
    int size;
    int shutting_down;
} RequestQueue;

RequestQueue queue;

int scheduling_rr = 0; // 0 = FCFS, 1 = RR
int running = 1;
int server_listen_fd = -1;

/* Forward declarations of helper functions already present in original file */
int generateUDPport(int *sock_udp);
int udp_communicate(int sent_port_no, int sock_udp_listen);

// Initialize the request queue
void queue_init(RequestQueue *q)
{
    q->head = q->tail = NULL;
    pthread_mutex_init(&q->lock, NULL);
    pthread_cond_init(&q->cond, NULL);
    q->size = 0;
    q->shutting_down = 0;
}

// Add a request to the queue
void queue_enqueue(RequestQueue *q, Request *r)
{
    r->next = NULL;
    pthread_mutex_lock(&q->lock);
    if (q->tail)
        q->tail->next = r;
    else
        q->head = r;
    q->tail = r;
    q->size++;
    pthread_cond_signal(&q->cond);
    pthread_mutex_unlock(&q->lock);
}

// Remove a request from the queue
Request *queue_dequeue(RequestQueue *q)
{
    pthread_mutex_lock(&q->lock);
    while (q->head == NULL && !q->shutting_down)
    {
        pthread_cond_wait(&q->cond, &q->lock);
    }
    if (q->shutting_down && q->head == NULL)
    {
        pthread_mutex_unlock(&q->lock);
        return NULL;
    }
    Request *r = q->head;
    q->head = r->next;
    if (q->head == NULL)
        q->tail = NULL;
    q->size--;
    pthread_mutex_unlock(&q->lock);
    return r;
}

// Wake waiting threads and stop the queue
void queue_shutdown(RequestQueue *q)
{
    pthread_mutex_lock(&q->lock);
    q->shutting_down = 1;
    pthread_cond_broadcast(&q->cond);
    pthread_mutex_unlock(&q->lock);
}

// Handle Ctrl+C shutdown
// Handle SIGINT and start graceful shutdown
void handle_sigint(int sig)
{
    (void)sig;
    printf("\nSIGINT received: shutting down server gracefully...\n");
    running = 0;
    if (server_listen_fd != -1)
        close(server_listen_fd);
    queue_shutdown(&queue);
}

// Handle one request with FCFS
// Process one client fully using FCFS
void process_request_full(Request *r)
{
    char buf[MAX_BUF_TCP];
    dprintf("Worker %ld: FCFS processing client fd %d\n", pthread_self(), r->client_fd);

    memset(buf, '\0', sizeof(buf));
    dprintf("Waiting to receive Phase1Message1 (TCP) from client fd %d...\n", r->client_fd);
    if (recv(r->client_fd, buf, MAX_BUF_TCP, 0) == 0)
    {
        perror("The client terminated prematurely\n");
        close(r->client_fd);
        free(r);
        return;
    }
    Message *Phase1Message1 = decodeCheckNPrint("Phase1Message1", MSG_TYPE_1, buf);

    int sock_udp_listen;
    int udp_no = generateUDPport(&sock_udp_listen);
    r->sock_udp_listen = sock_udp_listen;
    r->udp_port = udp_no;

    memset(buf, '\0', sizeof(buf));
    sprintf(buf, "%d", udp_no);
    Message *Phase1Message2 = create_msg(MSG_TYPE_2, strlen(buf), buf);

    if (send_msg_tcp("Phase1Message2", r->client_fd, Phase1Message2) != 1)
    {
        perror("cannot send");
        close(r->client_fd);
        free(r);
        return;
    }

    dprintf("Closing TCP socket %d with client\n", r->client_fd);
    close(r->client_fd);

    // Simulated work
    for (int i = 0; i < SERVICE_UNITS; ++i)
    {
        dprintf("Worker %ld: FCFS working on client %d unit %d/%d\n", pthread_self(), r->udp_port, i + 1, SERVICE_UNITS);
        sleep(1); // simulate work
    }

    // UDP phase
    udp_communicate(udp_no, sock_udp_listen);

    dprintf("Worker %ld: Finished FCFS client (udp %d)\n", pthread_self(), udp_no);
    free(r);
}

// Handle one request with RR
// Process one client using Round-Robin
void process_request_rr(Request *r)
{
    if (r->state == REQ_NEW)
    {
        // First slice: TCP handshake and init counter
        char buf[MAX_BUF_TCP];
        memset(buf, '\0', sizeof(buf));
        dprintf("Waiting to receive Phase1Message1 (TCP) from client fd %d...\n", r->client_fd);
        if (recv(r->client_fd, buf, MAX_BUF_TCP, 0) == 0)
        {
            perror("The client terminated prematurely\n");
            close(r->client_fd);
            free(r);
            return;
        }
        Message *Phase1Message1 = decodeCheckNPrint("Phase1Message1", MSG_TYPE_1, buf);

        int sock_udp_listen;
        int udp_no = generateUDPport(&sock_udp_listen);
        r->sock_udp_listen = sock_udp_listen;
        r->udp_port = udp_no;

        memset(buf, '\0', sizeof(buf));
        sprintf(buf, "%d", udp_no);
        Message *Phase1Message2 = create_msg(MSG_TYPE_2, strlen(buf), buf);

        if (send_msg_tcp("Phase1Message2", r->client_fd, Phase1Message2) != 1)
        {
            perror("cannot send");
            close(r->client_fd);
            free(r);
            return;
        }

        dprintf("Closing TCP socket %d with client\n", r->client_fd);
        close(r->client_fd);

        r->state = REQ_PROCESSING;
        r->units_remaining = SERVICE_UNITS;
        dprintf("RR: initialized request udp %d units_remaining=%d\n", r->udp_port, r->units_remaining);
    }

    // Do up to QUANTUM units of work
    int do_units = RR_QUANTUM;
    if (do_units > r->units_remaining)
        do_units = r->units_remaining;
    for (int i = 0; i < do_units; ++i)
    {
        dprintf("Worker %ld: RR processing udp %d, doing unit (before=%d)\n", pthread_self(), r->udp_port, r->units_remaining);
        sleep(1); // simulate one unit of work
        r->units_remaining--;
    }

    dprintf("Worker %ld: RR processed udp %d, units_remaining=%d\n", pthread_self(), r->udp_port, r->units_remaining);

    if (r->units_remaining > 0)
    {
        // Not done: re-enqueue for fairness
        queue_enqueue(&queue, r);
        dprintf("RR: requeued udp %d for further processing\n", r->udp_port);
    }
    else
    {
        // Done: do UDP phase
        udp_communicate(r->udp_port, r->sock_udp_listen);
        dprintf("Worker %ld: RR finished udp %d\n", pthread_self(), r->udp_port);
        free(r);
    }
}

// Worker thread
// Worker thread loop
void *worker_main(void *arg)
{
    (void)arg;
    while (1)
    {
        Request *r = queue_dequeue(&queue);
        if (r == NULL)
        {
            // queue is shutting down and empty
            break;
        }
        if (!scheduling_rr)
        {
            process_request_full(r);
        }
        else
        {
            process_request_rr(r);
        }
    }
    dprintf("Worker %ld exiting\n", pthread_self());
    return NULL;
}

// Helper functions

// Create a UDP socket and return its port
int generateUDPport(int *sock_udp)
{
    int sock_udp_listen;
    struct sockaddr_in thisAddr;
    if ((sock_udp_listen = socket(PF_INET, SOCK_DGRAM, 0)) == -1)
    {
        perror("cannot create udp listener socket");
        return 0;
    }
    else
    {
        dprintf("Created UDP socket %d (for request handler)\n", sock_udp_listen);
    }

    thisAddr.sin_family = PF_INET;
    thisAddr.sin_addr.s_addr = INADDR_ANY;
    thisAddr.sin_port = htons(0); // 0 lets OS choose port

    if ((bind(sock_udp_listen, (struct sockaddr *)&thisAddr, sizeof(thisAddr))) < 0)
    {
        perror("cannot bind to udp socket");
        close(sock_udp_listen);
        return 0;
    }

    struct sockaddr_in localSocket;
    socklen_t addressLength = sizeof localSocket;
    getsockname(sock_udp_listen, (struct sockaddr *)&localSocket, &addressLength);
    *sock_udp = sock_udp_listen;
    return (int)ntohs(localSocket.sin_port);
}

// Exchange messages on the UDP socket
int udp_communicate(int sent_port_no, int sock_udp_listen)
{
    int dataBytes, thatAddrLen;
    char buff[MAX_BUF_UDP];
    struct sockaddr_in thatAddr;
    thatAddrLen = sizeof(thatAddr);
    dprintf("Waiting to receive Phase2Message1 on udp port %d...\n", sent_port_no);

    if ((dataBytes = recvfrom(sock_udp_listen, buff, MAX_BUF_UDP - 1, 0, (struct sockaddr *)&thatAddr, &thatAddrLen)) < 0)
    {
        perror("cannot receive");
        close(sock_udp_listen);
        return 0;
    }

    buff[dataBytes] = '\0';
    Message *Phase2Message1 = decodeCheckNPrint("Phase2Message1", MSG_TYPE_3, buff);

    char *msg = "ACK message from server";
    Message *Phase2Message2 = create_msg(MSG_TYPE_4, strlen(msg), msg);

    if (send_msg_udp("Phase2Message2", sock_udp_listen, (struct sockaddr *)&thatAddr, sizeof(thatAddr), Phase2Message2) != 1)
    {
        perror("cannot send");
        close(sock_udp_listen);
        return 0;
    }
    close(sock_udp_listen);
    dprintf("Closed UDP socket %d\n", sock_udp_listen);
    return 0;
}

// Accept TCP and enqueue requests
// Entry point: start server and handle clients
int main(int argc, char **argv)
{
    int serv_port;
    if (argc != 3)
    {
        printf("Usage: %s <Server TCP Port> <fcfs|rr>\n", argv[0]);
        exit(1);
    }
    else
    {
        serv_port = atoi(argv[1]);
        if (strcmp(argv[2], "rr") == 0)
            scheduling_rr = 1;
        else
            scheduling_rr = 0;
    }

    signal(SIGINT, handle_sigint);

    queue_init(&queue);

    pthread_t workers[WORKER_COUNT];
    for (int i = 0; i < WORKER_COUNT; ++i)
    {
        if (pthread_create(&workers[i], NULL, worker_main, NULL) != 0)
        {
            perror("pthread_create");
            exit(1);
        }
    }

    int sock_tcp_listen;
    struct sockaddr_in cli_addr, serv_addr;
    int clilen;
    char buf[MAX_BUF_TCP];

    if ((sock_tcp_listen = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        printf("Cannot create socket\n");
        exit(0);
    }
    server_listen_fd = sock_tcp_listen;

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(serv_port);

    if (bind(sock_tcp_listen, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        printf("Unable to bind local address\n");
        exit(0);
    }

    if (listen(sock_tcp_listen, MAX_CLIENTS) < 0)
    {
        perror("Listen error");
        exit(1);
    }

    dprintf("Server listening on port %d (policy: %s)\n", serv_port, scheduling_rr ? "RR" : "FCFS");

    while (running)
    {
        clilen = sizeof(cli_addr);
        int child_sock_tcp = accept(sock_tcp_listen, (struct sockaddr *)&cli_addr, &clilen);
        if (child_sock_tcp < 0)
        {
            if (!running)
                break; // probably shutdown
            perror("Accept error");
            continue;
        }
        char *client_ip = inet_ntoa(cli_addr.sin_addr);
        int client_port = ntohs(cli_addr.sin_port);
        dprintf("Accepted connection: Client IP: %s  Client Port: %d  fd=%d\n", client_ip, client_port, child_sock_tcp);

        // Build request and enqueue
        Request *r = (Request *)malloc(sizeof(Request));
        r->client_fd = child_sock_tcp;
        r->client_addr = cli_addr;
        r->state = REQ_NEW;
        r->units_remaining = SERVICE_UNITS;
        r->sock_udp_listen = -1;
        r->udp_port = 0;
        r->next = NULL;
        queue_enqueue(&queue, r);
    }

    // Shutdown: stop workers and join
    dprintf("Main: closing listening socket and waiting for workers to finish...\n");
    if (server_listen_fd != -1)
        close(server_listen_fd);
    queue_shutdown(&queue);

    for (int i = 0; i < WORKER_COUNT; ++i)
    {
        pthread_join(workers[i], NULL);
    }

    dprintf("Server exited cleanly\n");
    return 0;
}
