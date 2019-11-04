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

#define GETAFFINITY(vertices, section) (vertices / section)  

// Next and Current level queue
typedef struct _thread_datastructure {
    int id;
    int * vertOffset;
    int * edgeArr;
    int n;
    int m;
    int nthreads;
} thread_ds;

typedef struct _message {
    int v;
    int parent;
    int dist;
} message;


int notDone =1;

int ** NQ;
int ** CQ;  
message ** SQ;

__thread int nqhead ;
__thread int cqhead ;
int * sqhead = NULL;

int * parentArr = NULL;
char * isVisited = NULL;
char * isEntered = NULL;
char ** isQueued = NULL;

int * distanceArr = NULL;

// Barrier
int barrierCnt;
pthread_mutex_t barrierMutex;

pthread_barrier_t barrierInit;
pthread_barrier_t barrierLvl;
pthread_barrier_t barrierLvl2;

void enque(int owner, int vertex, int parent, int dist, int n, int nthreads){
    assert(owner < nthreads);
    
    int vertexInmod = vertex % (n/nthreads+1);
    
    assert(vertexInmod < n/nthreads+1);

    if (! __sync_lock_test_and_set((isQueued[owner]+vertexInmod), 1)) {
        
        int sqlocal = __sync_fetch_and_add((sqhead+owner), 1);
        //message  msg;
        SQ[owner][sqlocal].v=vertex;
        SQ[owner][sqlocal].parent = parent;
        SQ[owner][sqlocal].dist = dist;
        //SQ[owner][sqlocal] = msg;    
        
        assert(vertex < n);
        assert(parent < n);
        assert(owner < nthreads);
        assert(sqlocal < n/nthreads+1);
    }
}




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

void bfsexplore(int * vertOffset, int * edgeArr, int n, int m, int threadId, int nthreads){
    int u = 0;
    int start, end =0;
    int owner = 0;
    int cqlocal = cqhead; cqhead--; //__sync_fetch_and_sub((cqhead+threadId), 1);
    cqlocal--;
    while( cqlocal+1 > 0){        
        u = CQ[threadId][cqlocal];
        
        if(isVisited[u] == 1) {
            cqlocal =cqhead; cqhead--;// __sync_fetch_and_sub(&cqhead, 1);
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

            owner = GETAFFINITY(v, ((n/nthreads)+1));
             
            if(owner != threadId){
                enque(owner, v, u, distanceArr[u]+1, n, nthreads);                
                
            } else if (!isVisited[v] && !isEntered[v] ) {
                isEntered[v] = 1;
   
                int nqlocal = nqhead; nqhead++; //= __sync_fetch_and_add(&(nqhead), 1);
                NQ[threadId][nqlocal] = v;
                    
                parentArr[v] = u;
                distanceArr[v] = distanceArr[u]+1;

                assert(distanceArr[v] >= 0);
                assert(v < n);
                assert(u < n);
                assert(nqlocal < n/nthreads+1);
  

                //printf("[%d] Node visited : %d\n", threadId, v);
            } else {
                //printf("[%d] Node ignored : %d\n", threadId, v);
            }
        }
        isVisited[u] = 1;
        
        cqlocal =cqhead; cqhead--;// __sync_fetch_and_sub(cqhead+threadId, 1);
        cqlocal--;
    }
}

void mainbfs(int * vertOffset, int * edgeArr, int n, int m, int nthreads){
    
    int init = 0;
    
    // Init barrier here
    while ( notDone ) {        
        if(!init){
            init = 1;
            pthread_barrier_wait(&barrierInit);
        }

        bfsexplore(vertOffset, edgeArr, n, m, 0, nthreads);
      
        pthread_barrier_wait(&barrierLvl);
        
        while (sqhead[0]>0){
            sqhead[0]--;
            int v = SQ[0][sqhead[0]].v;
            if (!isVisited[v] && !isEntered[v] ) {
                isEntered[v] = 1;

                NQ[0][nqhead] = v;
                nqhead++;

                parentArr[v] = SQ[0][sqhead[0]].parent;
                distanceArr[v] = SQ[0][sqhead[0]].dist;
                
                assert(distanceArr[v] >= 2);
                assert(v < n);
                assert(parentArr[v] < n);
                assert(nqhead-1 < n/nthreads+1);
            }            
        }
        
        int * tmp = CQ[0];
        CQ[0] = NQ[0];
        NQ[0] = tmp;
        cqhead = nqhead;
        nqhead = 0;
        
        notDone = 0;
        pthread_barrier_wait(&barrierLvl2);        
        __sync_fetch_and_or(&notDone, !(cqhead == 0) );
        pthread_barrier_wait(&barrierLvl2);
        //printf("[%d] Notdone : %d\n", 0, notDone);
    }


}


void bfsworker(int * vertOffset, int * edgeArr, int n, int m, int threadId, int nthreads){
    
    //printf("[%d] Waiting for job\n", threadId);
    pthread_barrier_wait(&barrierInit);

    //printf("[%d] Enter main thread\n", threadId);
    while (notDone ){
        
        bfsexplore(vertOffset, edgeArr, n, m, threadId, nthreads);
        
        // The first barrier is to ensure that we get final value of nqhead and cqhead
        pthread_barrier_wait(&barrierLvl);
        // The last barrier is to ensure that CQ  and NQ are swapped and cqhead and nqhead are swapped before starting a work

        while (sqhead[threadId]>0){
            sqhead[threadId]--;
         
            int v = SQ[threadId][sqhead[threadId]].v;
            if (!isVisited[v] && !isEntered[v] ) {
                isEntered[v] = 1;

                NQ[threadId][nqhead] = v;
                nqhead++;
                
                parentArr[v] = SQ[threadId][sqhead[threadId]].parent;
                distanceArr[v] = SQ[threadId][sqhead[threadId]].dist;
                
                assert(distanceArr[v] >= 0);
                assert(v < n);
                assert(parentArr[v] < n);
                assert(nqhead-1 < n/nthreads+1);
            }
            
        }



        int * tmp = CQ[threadId];
        CQ[threadId] = NQ[threadId];
        NQ[threadId] = tmp;
        cqhead = nqhead;
        nqhead = 0;
       
        pthread_barrier_wait(&barrierLvl2);
        __sync_fetch_and_or(&notDone, !(cqhead == 0) );
        pthread_barrier_wait(&barrierLvl2);
        //printf("[%d] Notdone : %d\n", 0, notDone);
    }
    
}

void* bfsScheduler (void * arg){
    thread_ds * tds = (thread_ds *)arg;    
    int threadId = tds->id;
    int * vertOffset = tds->vertOffset;
    int * edgeArr = tds->edgeArr;
    int n = tds->n;
    int m = tds->m;
    int nthreads = tds->nthreads;

    pincore(threadId);        
    //printf("[%d] bfsworker\n", threadId);

#if 1
    
    CQ[threadId] = calloc(n/nthreads+1, sizeof(int));
    NQ[threadId] = calloc(n/nthreads+1, sizeof(int));
    SQ[threadId] = calloc(n/nthreads+1, sizeof(message));
    isQueued[threadId] = calloc(n/nthreads+1, sizeof(char));
    memset(isQueued[threadId], 0, (n/nthreads+1) * sizeof(char));

#else

    CQ[threadId] = calloc(n+1, sizeof(int));
    NQ[threadId] = calloc(n+1, sizeof(int));
    SQ[threadId] = calloc(n+1, sizeof(message));

#endif

    nqhead = 0;
    cqhead = 0;
    sqhead[threadId] = 0;
    bfsworker(vertOffset, edgeArr, n, m, threadId, nthreads);
    
    //printf("Thread : %d finished\n", threadId);
    return NULL;
}


void parallelbfs(int * vertOffset, int * edgeArr, int n, int m, int nthreads ) {    
    parentArr = calloc(n, sizeof(int));
    isVisited = calloc(n, sizeof(char));
    isEntered = calloc(n, sizeof(char));
    
    isQueued = calloc(nthreads, sizeof(char*));

    //nqhead = calloc(nthreads, sizeof(int));
    //cqhead = calloc(nthreads, sizeof(int));
    sqhead = calloc(nthreads, sizeof(int));

    //int rem = n % 8;
    //int nBytesAlloc = n/8 + !!rem;
    
    distanceArr = calloc(n, sizeof(int));
    CQ = calloc(nthreads, sizeof(int*));
    NQ = calloc(nthreads, sizeof(int*));
    SQ = calloc(nthreads, sizeof(message*));
    
    // Has a barrier here
    pincore(0);
 
    CQ[0] = calloc(n/nthreads+1, sizeof(int));
    NQ[0] = calloc(n/nthreads+1, sizeof(int));
    SQ[0] = calloc(n/nthreads+1, sizeof(message));
    isQueued[0] = calloc(n/nthreads+1, sizeof(char));

    nqhead = 0;
    cqhead = 0;
    sqhead[0] = 0;

    memset(parentArr, INT_MAX, n * sizeof(int));
    memset(distanceArr, INT_MAX, n * sizeof(int));
    memset(isVisited, 0, n * sizeof(char));
    memset(isEntered, 0, n * sizeof(char));
    memset(isQueued[0], 0, (n/nthreads+1) * sizeof(char));

    parentArr[0] = 0;
    distanceArr[0] = 0;

    CQ[0][cqhead] = 0;
    cqhead++;
    
    mainbfs(vertOffset, edgeArr, n, m, nthreads);


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
        tds[i-1].nthreads = nthreads;
        pthread_create(threads+i-1, NULL, bfsScheduler, (void*)(tds+i-1));
    }
    //pincore(0);
    
    // Time here
    struct timespec start, end; 
    
    clock_gettime(CLOCK_MONOTONIC, &start);
    parallelbfs(vertOffset, edgeArr, n, m, nthreads);    
    clock_gettime(CLOCK_MONOTONIC, &end);

    double time_taken; 
    time_taken = (end.tv_sec - start.tv_sec) * 1e9; 
    time_taken = (time_taken + (end.tv_nsec - start.tv_nsec)) * 1e-9; 
 
    //printf("Main thread. notDone :  %d\n", notDone);
    for (int i=1; i<nthreads; i++){
        pthread_join(*(threads+i-1), NULL);
    }

#if 0
    for (int i = 0; i<n; i++) {
        //printf ("Node : %d. Distance : %d Parent : %d\n", i, distanceArr[i], parentArr[i]);
        printf ("Node : %d. Distance : %d\n", i, distanceArr[i]);
    }
#endif
    printf("Time taken : %0.9lf s\n", time_taken);

    for(int i = 0; i<nthreads; i++){
        if(NQ[i]) free(NQ[i]);
        if(CQ[i]) free(CQ[i]);
        if(SQ[i]) free(SQ[i]);
        if(isQueued[i]) free(isQueued[i]);
    }

    if(NQ) free(NQ);
    if(CQ) free(CQ);
    if(SQ) free(SQ);
    if(isQueued) free(isQueued);

    if(tds) free(tds);

    if (distanceArr) free(distanceArr);
    if (isVisited) free(isVisited);
    if (isEntered) free(isEntered);

    if (parentArr) free(parentArr);

    //free(nqhead);
    //free(cqhead);
    free(sqhead);

    free(vertOffset);
    free(edgeArr);
    fclose(graphin);
    
    return 0;
}
