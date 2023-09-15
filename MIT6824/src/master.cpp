#include "master.h"
Master::Master(int mapnum, int reducenum)
{
    map_num = mapnum;
    reduce_num = reducenum;
    curMapIndex = 0;
    curReduceIndex = 0;
    done = false;
    finishedMapTask.clear();
    finishedReduceTask.clear();
    runningmap_vec.clear();
    runningreduce_vec.clear();
    m_list.clear();
    for (int i = 0; i < reduce_num; i++)
    {
        reduce_index.emplace_back(i);
    }
}
void Master::GetFile(char *file[], int index)
{
    for (int i = 1; i < index; i++)
    {
        m_list.emplace_back(file[i]);
    }
    file_num = index - 1;
}
bool Master::isReduceDone()
{
    pthread_mutex_lock(&assigntask_lock);
    int num = finishedReduceTask.size();
    pthread_mutex_unlock(&assigntask_lock);
    return (num == reduce_num);
}
std::string Master::AssignmapTasks()
{
    if (!isMapDone())
        return "empty";
    if (!m_list.empty())
    {
        pthread_mutex_lock(&assigntask_lock);
        char *task = m_list.back();
        m_list.pop_back();
        pthread_mutex_unlock(&assigntask_lock);
        waitmap(std::string(task));
        return std::string(task);
    }
    return "empty";
}
int Master::AssignreduceTasks()
{
    if (!isReduceDone())
        return -1;
    if (!reduce_index.empty())
    {
        pthread_mutex_lock(&assigntask_lock);
        int reduceindex = reduce_index.back();
        reduce_index.pop_back();
        pthread_mutex_unlock(&assigntask_lock);
        waitReduce(reduceindex);
        return reduceindex;
    }
}
void Master::waitReduce(int reduceindex)
{
    pthread_mutex_lock(&assigntask_lock);
    runningreduce_vec.push_back(reduceindex);
    pthread_mutex_unlock(&assigntask_lock);
    pthread_t tid;
    pthread_create(&tid, NULL, waitReduceTask, this);
    pthread_detach(tid);
}
void *Master::waitReduceTask(void *arg)
{
    Master *reduce = (Master *)arg;
    void *status; // 用于接收线程的状态
    pthread_t tid;
    char *p;
    *p = 'r'; // 参数
    pthread_create(&tid, NULL, waitTime, &p);
    pthread_join(tid, &status);
    pthread_mutex_lock(&reduce->assigntask_lock);
    if (reduce->finishedReduceTask.count(reduce->runningreduce_vec[reduce->curReduceIndex]) == 0)
    {
        // 超时，任务没有完成
        std::cout << "reduce :" << (reduce->runningreduce_vec[reduce->curReduceIndex]) << "reduce time out." << std::endl;
        // const char *tmp = map->runningmap_vec[map->curMapIndex].c_str();
        reduce->reduce_index.emplace_back(reduce->runningreduce_vec[reduce->curReduceIndex]);
    }
    else
    {
        std::cout << "file:" << (reduce->runningreduce_vec[reduce->curReduceIndex]) << "reduce finished." << std::endl;
    }
    reduce->curReduceIndex++;
    pthread_mutex_unlock(&reduce->assigntask_lock);
}
void Master::waitmap(std::string filename)
{
    pthread_mutex_lock(&assigntask_lock);
    runningmap_vec.push_back(filename);
    pthread_mutex_unlock(&assigntask_lock);
    pthread_t pid;
    pthread_create(&pid, NULL, waitMapTask, this);
    pthread_detach(pid);
}
void *Master::waitMapTask(void *arg)
{
    Master *map = (Master *)arg;
    void *status; // 用于接收线程的状态
    pthread_t tid;
    char *p;
    *p = 'm'; // 参数
    pthread_create(&tid, NULL, waitTime, &p);
    pthread_join(tid, &status);
    pthread_mutex_lock(&map->assigntask_lock);
    if (map->finishedMapTask.count(map->runningmap_vec[map->curMapIndex]) == 0)
    {
        // 超时，任务没有完成
        std::cout << "file :" << (map->runningmap_vec[map->curMapIndex]).c_str() << "map time out." << std::endl;
        const char *tmp = map->runningmap_vec[map->curMapIndex].c_str();

        map->m_list.push_back((char *)tmp);
    }
    else
    {
        std::cout << "file:" << (map->runningmap_vec[map->curMapIndex]).c_str() << "map finished." << std::endl;
    }
    map->curMapIndex++;
    pthread_mutex_unlock(&map->assigntask_lock);
}
void *waitTime(void *arg)
{
    char *op = (char *)(arg);
    if (*op == 'm')
    {
        sleep(MAPTIMEOUT); // 分map和reduce进行线程休眠
    }
    if (*op == 'r')
    {
        sleep(REDUCETIMEOUT);
    }
}
bool Master::isMapDone(){
    pthread_mutex_lock(&assigntask_lock);
    int num=finishedMapTask.size();
    pthread_mutex_unlock(&assigntask_lock);
    return (num==file_num);
    
}
void Master::setReduceStat(int taskIndex){
    pthread_mutex_lock(&assigntask_lock);
    finishedReduceTask[taskIndex]=1;//更新为已完成
    pthread_mutex_unlock(&assigntask_lock);
}
int Master::GetMapNum(){
    return map_num;
}
int Master::GetReduceNum(){
    return reduce_num;
}
void Master::setMapStat(std::string filename){
    pthread_mutex_lock(&assigntask_lock);
    finishedMapTask[filename]=1;
    pthread_mutex_unlock(&assigntask_lock);
}