#define _GNU_SOURCE
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <malloc.h>
#include <time.h>
#include <unistd.h>
#define THREADS 8
#define COUNT 1024
#define ROUNDS 8
static pthread_barrier_t barrier;
static int trim_enabled;
static size_t retained[ROUNDS];
static double trim_ms[ROUNDS];
static double now(void){struct timespec t;clock_gettime(CLOCK_MONOTONIC,&t);return t.tv_sec+t.tv_nsec/1e9;}
static size_t rss(void){FILE*f=fopen("/proc/self/statm","r");size_t v,r;fscanf(f,"%zu %zu",&v,&r);fclose(f);return r*(size_t)sysconf(_SC_PAGESIZE);}
static void *worker(void *id){
 void *anchors[ROUNDS][COUNT];
 for(int round=0;round<ROUNDS;round++){
  void*warm=malloc(4*1024*1024);memset(warm,1,4*1024*1024);free(warm);
  void*blocks[COUNT];
  for(int i=0;i<COUNT;i++){size_t len=i%2?16*1024:256*1024;blocks[i]=malloc(len);memset(blocks[i],7,len);anchors[round][i]=malloc(32);memset(anchors[round][i],3,32);}
  for(int i=0;i<COUNT;i++)free(blocks[i]);
  pthread_barrier_wait(&barrier);
  if((intptr_t)id==0){double t=now();if(trim_enabled==1 || (trim_enabled==2 && round==ROUNDS-1))malloc_trim(0);trim_ms[round]=(now()-t)*1000;retained[round]=rss();}
  pthread_barrier_wait(&barrier);
 }
 for(int r=0;r<ROUNDS;r++)for(int i=0;i<COUNT;i++)free(anchors[r][i]);
 return NULL;
}
int main(int argc,char**argv){trim_enabled=argc>1?atoi(argv[1]):0;pthread_t threads[THREADS];pthread_barrier_init(&barrier,NULL,THREADS);double t=now();for(int i=0;i<THREADS;i++)pthread_create(&threads[i],NULL,worker,(void*)(intptr_t)i);for(int i=0;i<THREADS;i++)pthread_join(threads[i],NULL);double ms=(now()-t)*1000;struct mallinfo2 m=mallinfo2();printf("trim=%d elapsed_ms=%.3f arena=%zu free=%zu rss=%zu round_rss=",trim_enabled,ms,m.arena,m.fordblks,rss());for(int i=0;i<ROUNDS;i++)printf("%zu,",retained[i]);printf(" trim_ms=");for(int i=0;i<ROUNDS;i++)printf("%.3f,",trim_ms[i]);puts("");}
