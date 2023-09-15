#ifndef WOEKER_H
#define WORKER_H
#include "map_reduceFun.h"
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <semaphore.h>
#include<string.h>
#include"../../buttonrpc-master/buttonrpc.hpp"
#define MAX_REDUCE_NUM 16
// 获取文件信息，储存为keyvalue
class Worker
{
public:
    Worker(int mn,int rn);
    pthread_mutex_t map_mutex;
    pthread_cond_t cond;
    int map_num;
    int reduce_num;
    int mapid;
    int mapdisableid;//与超时有关的变量
    int reducedisableid;//与超时有关的变量
    void rmFiles();
    void removeOutputFiles();
    void writeKV(int &fd,const KeyValue v);
    KeyValue GetContent(char *filename);
    void CreateThread();
    int iHash(std::string task);//为某个key计算对应reduce的索引值
    void writeInDisk(const std::vector<KeyValue>&kv,int maptaskidx);
    static void* mapWorker(void* arg);
    static void* reduceWorker(void* arg);
    ~Worker();
};

#endif