#ifndef MASTER_H
#define MASTER_H
#include <vector>
#include <list>
#include <pthread.h>
#include <semaphore.h>
#include <iostream>
#include <string>
#include <unordered_map>
#include <unistd.h>
#include "../../buttonrpc-master/buttonrpc.hpp"
#define MAPTIMEOUT 4
#define REDUCETIMEOUT 5
class Master
{
public:
    void GetFile(char *file[], int index); // 获取终端文件信息
    std::string AssignmapTasks();          // 分配map的工作
    int AssignreduceTasks();               // 分配reduce的工作
    bool isMapDone();
    bool isReduceDone();
    void waitmap(std::string filename);
    void waitReduce(int reduceindex);
    void setReduceStat(int taskIndex);
    void setMapStat(std::string filename);
    Master(int mapnum, int reducenum);
    static void *waitMapTask(void *arg);
    static void *waitReduceTask(void *arg);
    static void *waitTime(void *arg);
    int GetMapNum();
    int GetReduceNum();
private:
    int map_num;    // map_work的个数
    int reduce_num; // reduce_work的数目
    int file_num;
    int done; // 完成工作的数目
    int curMapIndex;
    int curReduceIndex;

    std::vector<std::string> runningmap_vec;
    std::vector<int> runningreduce_vec;
    std::list<char *> m_list;                             // 所有map任务
    std::vector<int> reduce_index;                        // 所有reduce任务
    pthread_mutex_t assigntask_lock;                      // 分配任务的锁
    std::unordered_map<std::string, int> finishedMapTask; // 存放所有完成的map任务对应的文件名
    std::unordered_map<int, int> finishedReduceTask;      // 存放所有完成的reduce任务对应的reduce编号
   
};
#endif