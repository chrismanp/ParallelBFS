#define _GNU_SOURCE
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <malloc.h>
#include <limits.h>
#include <pthread.h>

// Next and Current level queue
typedef struct _thread_datastructure {
    int id;
    int * vertOffset;
    int * edgeArr;
    int n;
    int m;
} thread_ds;

int notDone =1;

int * NQ;
int * CQ;  

int nqhead = 0;
int nqtail = 0;

int cqhead = 0;
int cqtail = 0;

int * parentArr = NULL;
char * isVisited = NULL;
int * distanceArr = NULL;

// Barrier
int barrierCnt;
pthread_mutex_t barrierMutex;
pthread_mutex_t queueMutex;

pthread_barrier_t barrierInit;
pthread_barrier_t barrierLvl;
pthread_barrier_t barrierLvl2;


void waitbarrier(int * bar){
    pthread_mutex_lock(&barrierMutex);
    (*bar)--;
    pthread_mutex_unlock(&barrierMutex);
    
    while (*bar > 0) {
        sched_yield();
    }
    
}

void initbarrier(int * bar, int count){
    pthread_mutex_init(&barrierMutex, NULL);
    *bar = count;
}

void pincore(int threadId){    
    cpu_set_t cpuset;
    
    CPU_ZERO(&cpuset);
    CPU_SET(threadId, &cpuset);
    
    pthread_t tid = pthread_self();
    int res = pthread_setaffinity_np(tid, sizeof(cpu_set_t), &cpuset);
    
    assert (res == 0);
    waitbarrier(&barrierCnt);
}

void bfswrapper(int * vertOffset, int * edgeArr, int n, int m, int nthreads){
    int u = 0;
    int start, end =0;

    int init = 0;
    int activatelvl = 0;
   
    // Init barrier here
    while ( cqhead > 0) {        
        if(!init){
            init = 1;
            pthread_barrier_wait(&barrierInit);
        }

        if(activatelvl){
            pthread_barrier_wait(&barrierLvl2);
            activatelvl = 0;
        }

        pthread_mutex_lock(&queueMutex);
      
        while( cqhead > 0){        
            u = CQ[cqhead-1];
            cqhead--;
            pthread_mutex_unlock(&queueMutex);

            if(isVisited[u] == 1) {
                continue;
            }

            printf("[%d] Parent node u : %d index :%d\n", 0, u, cqhead);
            start = vertOffset[u];
            if ( u < n-1) {
                end = vertOffset[u+1];
            } else {
                end = m;
            }
            
            for(int i = start; i<end; i++){            

                int v = edgeArr[i];                
                if (isVisited[v] == 0) {
                    
                    pthread_mutex_lock(&queueMutex);                    
                    NQ[nqhead] = v;
                    nqhead++;
                    pthread_mutex_unlock(&queueMutex);
                    
                    parentArr[v] = u;
                    distanceArr[v] = distanceArr[u]+1;
                    

                    printf("[%d] Node visited : %d\n", 0, v);
                } else {
                    printf("[%d] Node ignored : %d\n", 0, v);
                }
            }
            isVisited[u] = 1;            
        }
        pthread_mutex_unlock(&queueMutex);

        // Sync'ed
        printf("Wait notDone : %d ThreadId: %d\n", notDone, 0);        
        
        pthread_barrier_wait(&barrierLvl);


        int * tmp = CQ;
        CQ = NQ;
        NQ = tmp;
        cqhead = nqhead;
        nqhead = 0;

        activatelvl = 1;
       
                
    }


}


void bfsworker(int * vertOffset, int * edgeArr, int n, int m, int threadId){
    int u = 0;
    int start, end =0;
    
    printf("[%d] Waiting for job\n", threadId);
    pthread_barrier_wait(&barrierInit);

    printf("[%d] Enter main thread\n", threadId);
    while (notDone ){
        pthread_mutex_lock(&queueMutex);
        while( cqhead > 0 ){        
            u = CQ[cqhead-1];
            cqhead--;
            pthread_mutex_unlock(&queueMutex);

            if(isVisited[u] == 1) {
                continue;
            }

            printf("[%d] Parent node u : %d index :%d\n", threadId, u, cqhead);
            start = vertOffset[u];
            if ( u < n-1) {
                end = vertOffset[u+1];
            } else {
                end = m;
            }
            
            for(int i = start; i<end; i++){            
                int v = edgeArr[i];                
                if (isVisited[v] == 0) {
                    
                    pthread_mutex_lock(&queueMutex);
                    NQ[nqhead] = v;
                    nqhead++;
                    pthread_mutex_unlock(&queueMutex);

                    parentArr[v] = u;
                    distanceArr[v] = distanceArr[u]+1;
                    printf("[%d] Node visited : %d\n", threadId, v);
                } else {
                    printf("[%d] Node ignored : %d\n", threadId, v);
                }
            }
            isVisited[u] = 1;            
        }
        pthread_mutex_unlock(&queueMutex);
        printf("Wait notDone : %d ThreadId: %d\n", notDone, threadId);
        
        pthread_barrier_wait(&barrierLvl);
        pthread_barrier_wait(&barrierLvl2);
    }
    
}

void* bfsScheduler (void * arg){
    thread_ds * tds = (thread_ds *)arg;    
    int threadId = tds->id;
    int * vertOffset = tds->vertOffset;
    int * edgeArr = tds->edgeArr;
    int n = tds->n;
    int m = tds->m;

    pincore(threadId);        
    printf("[%d] bfsworker\n", threadId);
    bfsworker(vertOffset, edgeArr, n, m, threadId);
    
    //free(tds);
    printf("Thread : %d finished\n", threadId);
    return NULL;
}


void parallelbfs(int * vertOffset, int * edgeArr, int n, int m, int nthreads ) {    
    parentArr = calloc(n, sizeof(int));
    isVisited = calloc(n, sizeof(char));
    distanceArr = calloc(n, sizeof(int));
    CQ = calloc(n, sizeof(int));
    NQ = calloc(n, sizeof(int));

    for (int i =0; i<n; i++){
        parentArr[i] = INT_MAX;
        isVisited[i] = 0;
        distanceArr[i] = INT_MAX;
    }
    
    parentArr[0] = 0;
    distanceArr[0] = 0;

    CQ[cqhead] = 0;
    cqhead++;
    

    pthread_mutex_init(&queueMutex, NULL);
    bfswrapper(vertOffset, edgeArr, n, m, nthreads);


}


void parseGraphIntoAdjArr(FILE * graphin, int ** vertOffset, int ** edgeArr, 
                          int * n, int * m) {
#define N 100
    char * line = calloc(N, sizeof(char));
    size_t len = N;

    int i = 0;

    fgets(line, len, graphin);
    if( strcmp(line, "AdjacencyGraph\n") != 0 ){
        printf("Incorrect format : %s\n", line);
        free(line);
        exit( EXIT_FAILURE);
    }

    
    fgets(line, len, graphin);
    *n = atoi(line);
    fgets(line, len, graphin);
    *m = atoi(line);

    *vertOffset = calloc(*n, sizeof(int));
    *edgeArr    = calloc(*m, sizeof(int));

    while ( fgets(line, len, graphin) != NULL ) {
        if(i < *n){
            (*vertOffset)[i] = atoi(line);
        } else {
            (*edgeArr)[i-*n] = atoi(line);
        }
        i++;        
    }

    free(line);

    return;
}


int main ( int argc, char * argv[]){
    
    if(argc != 3){
        printf("Argument incorrect. Usage ./bfs File-AdjArray nthreads \n");
        exit(EXIT_FAILURE);
    }

    // read the input 
    FILE * graphin = fopen(argv[1], "r");    
    if(!graphin){
        exit(EXIT_FAILURE);
    }
    int nthreads = atoi(argv[2]);

    int * vertOffset = NULL;
    int * edgeArr = NULL;

    int n = 0; // Number of vertices
    int m = 0; // Number of edges
    
    parseGraphIntoAdjArr(graphin, &vertOffset, &edgeArr, &n, &m);
    
    printf("Number of vertices : %d and edges : %d\n", n, m); 
    printf("Vertices array :");

    for(int i = 0; i<n; i++){
        printf ("%d ", vertOffset[i]); 
    }
    printf("\n");
    
    printf("Edge array :");

    for(int i = 0; i<m; i++){
        printf ("%d ", edgeArr[i]); 
    }
    printf("\n");

    pthread_barrier_init(&barrierLvl, NULL, nthreads);
    pthread_barrier_init(&barrierLvl2, NULL, nthreads);
    pthread_barrier_init(&barrierInit, NULL, nthreads);

    // Create n thread and pin it to several cores
    initbarrier(&barrierCnt, nthreads);
    pthread_t* threads = calloc(nthreads-1, sizeof(pthread_t));
    
    thread_ds * tds;
    tds = calloc(nthreads-1, sizeof(thread_ds));    
    for (int i=1; i<nthreads; i++) {
        tds[i-1].id = i;
        tds[i-1].vertOffset = vertOffset;
        tds[i-1].edgeArr = edgeArr;
        tds[i-1].n = n;
        tds[i-1].m = m;

        pthread_create(threads+i-1, NULL, bfsScheduler, (void*)(tds+i-1));
    }
    pincore(0);
    
    parallelbfs(vertOffset, edgeArr, n, m, nthreads);
    
    notDone = 0;
   
    pthread_barrier_wait(&barrierLvl2);

    printf("Main thread. notDone :  %d\n", notDone);

    for (int i=1; i<nthreads; i++){
        pthread_join(*(threads+i-1), NULL);
    }

    free(tds);
    
    for (int i = 0; i<n; i++) {
        printf ("Node : %d. Distance : %d Parent : %d\n", i, distanceArr[i], parentArr[i]);
    }

    
    if (distanceArr) free(distanceArr);
    if (isVisited) free(isVisited);
    if (parentArr) free(parentArr);
    
    //free(vertOffset);
    //free(edgeArr);
    //fclose(graphin);
    
    return 0;
}