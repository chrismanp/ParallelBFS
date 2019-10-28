#define _GNU_SOURCE
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <malloc.h>
#include <limits.h>
#include <pthread.h>

#define SET(bitVector, location) ( bitVector[location/8] = bitVector[location/8] | ((char) 1 << (location % 8) ) )

#define CLEAR(bitVector, location) ( bitVector[location/8] = bitVector[location/8] & !((char) 1 << (location % 8) ) )

#define GET(bitVector, location) ( ( bitVector[location/8] >> (location % 8) ) & 0x1  ) 

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
char * isEntered = NULL;

int * distanceArr = NULL;

// Barrier
int barrierCnt;
pthread_mutex_t barrierMutex;

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
        int cqlocal = __sync_fetch_and_sub(&cqhead, 1);
        cqlocal--;
        while( cqlocal+1 > 0){        
            u = CQ[cqlocal];
            
            if(isVisited[u]) {
                cqlocal = __sync_fetch_and_sub(&cqhead, 1);
                cqlocal--;
                continue;
            }

            //printf("[%d] Parent node u : %d index :%d\n", 0, u, cqlocal);
            start = vertOffset[u];
            if ( u < n-1) {
                end = vertOffset[u+1];
            } else {
                end = m;
            }
            
            for(int i = start; i<end; i++){            

                int v = edgeArr[i];                
                if (!isVisited[v] && !isEntered[v])  {
                    isEntered[v] = 1;
   
                    int nqlocal = __sync_fetch_and_add(&nqhead, 1);
                    NQ[nqlocal] = v;
                    
                    parentArr[v] = u;
                    distanceArr[v] = distanceArr[u]+1;


                    assert(v < n);
                    assert(u < n);
                    assert(nqlocal < n);
                    //printf("[%d] Node visited : %d\n", 0, v);
                } else {
                    //printf("[%d] Node ignored : %d\n", 0, v);
                }
            }
            isVisited[u] = 1;
            cqlocal = __sync_fetch_and_sub(&cqhead, 1);
            cqlocal--;
            //printf("cqhead : %d cqlocal : %d\n", cqhead, cqlocal);
        }

        //printf("Wait notDone : %d ThreadId: %d\n", notDone, 0);        
        
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
    
    //printf("[%d] Waiting for job\n", threadId);
    pthread_barrier_wait(&barrierInit);

    //printf("[%d] Enter main thread\n", threadId);
    while (notDone ){


        int cqlocal = __sync_fetch_and_sub(&cqhead, 1);
        cqlocal--;
        while( cqlocal+1 > 0){        
            u = CQ[cqlocal];
            
            if(isVisited[u] == 1) {
                cqlocal = __sync_fetch_and_sub(&cqhead, 1);
                cqlocal--;
                continue;
            }

            //printf("[%d] Parent node u : %d index :%d\n", threadId, u, cqlocal);
            start = vertOffset[u];
            if ( u < n-1) {
                end = vertOffset[u+1];
            } else {
                end = m;
            }
            
            for(int i = start; i<end; i++){            
                int v = edgeArr[i];                
                if (!isVisited[v] && !isEntered[v] ) {
                    isEntered[v] = 1;
   

                    int nqlocal = __sync_fetch_and_add(&nqhead, 1);
                    NQ[nqlocal] = v;
                    
                    parentArr[v] = u;
                    distanceArr[v] = distanceArr[u]+1;
                    //printf("[%d] Node visited : %d\n", threadId, v);
                } else {
                    //printf("[%d] Node ignored : %d\n", threadId, v);
                }
            }
            isVisited[u] = 1;
            cqlocal = __sync_fetch_and_sub(&cqhead, 1);
            cqlocal--;
        }
        //printf("Wait notDone : %d ThreadId: %d\n", notDone, threadId) 

        // The first barrier is to ensure that we get final value of nqhead and cqhead
        pthread_barrier_wait(&barrierLvl);
        // The last barrier is to ensure that CQ  and NQ are swapped and cqhead and nqhead are swapped before starting a wrok
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
    //printf("[%d] bfsworker\n", threadId);
    bfsworker(vertOffset, edgeArr, n, m, threadId);
    
    //printf("Thread : %d finished\n", threadId);
    return NULL;
}


void parallelbfs(int * vertOffset, int * edgeArr, int n, int m, int nthreads ) {    
    parentArr = calloc(n, sizeof(int));
    isVisited = calloc(n, sizeof(char));
    isEntered = calloc(n, sizeof(char));
    
    //int rem = n % 8;
    //int nBytesAlloc = n/8 + !!rem;
    
    distanceArr = calloc(n, sizeof(int));
    CQ = calloc(n, sizeof(int));
    NQ = calloc(n, sizeof(int));
    
    memset(parentArr, INT_MAX, n * sizeof(int));
    memset(distanceArr, INT_MAX, n * sizeof(int));
    memset(isVisited, 0, n * sizeof(char));
    memset(isEntered, 0, n * sizeof(char));

    parentArr[0] = 0;
    distanceArr[0] = 0;

    CQ[cqhead] = 0;
    cqhead++;
    
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

#if 0    
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
#endif

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
    
    // Time here
    struct timespec start, end; 
    
    clock_gettime(CLOCK_MONOTONIC, &start);
    parallelbfs(vertOffset, edgeArr, n, m, nthreads);    
    clock_gettime(CLOCK_MONOTONIC, &end);

    double time_taken; 
    time_taken = (end.tv_sec - start.tv_sec) * 1e9; 
    time_taken = (time_taken + (end.tv_nsec - start.tv_nsec)) * 1e-9; 
 

    notDone = 0;   
    pthread_barrier_wait(&barrierLvl2);

    //printf("Main thread. notDone :  %d\n", notDone);
    for (int i=1; i<nthreads; i++){
        pthread_join(*(threads+i-1), NULL);
    }

    
    for (int i = 0; i<n; i++) {
        //printf ("Node : %d. Distance : %d Parent : %d\n", i, distanceArr[i], parentArr[i]);
        printf ("Node : %d. Distance : %d\n", i, distanceArr[i]);
    }

    printf("Time taken : %0.9lf s\n", time_taken);

    if(NQ) free(NQ);
    if(CQ) free(CQ);
    
    if(tds) free(tds);

    if (distanceArr) free(distanceArr);
    if (isVisited) free(isVisited);
    if (isEntered) free(isEntered);

    if (parentArr) free(parentArr);
    
    free(vertOffset);
    free(edgeArr);
    fclose(graphin);
    
    return 0;
}
