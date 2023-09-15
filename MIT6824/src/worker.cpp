#include "worker.h"
Worker::Worker(int mn, int rn)
{
    map_num = mn;
    reduce_num = rn;
    mapid=0;
    mapdisableid=0;
    reducedisableid=0;
}
void Worker::CreateThread()
{
    pthread_t mapthread[map_num];
    pthread_t reducethread[reduce_num];
    for (int i = 0; i < map_num; ++i)
    {
        pthread_create(&mapthread[i], NULL, mapWorker, this);
        pthread_detach(mapthread[i]);
    }//
    pthread_mutex_lock(&map_mutex);
    pthread_cond_wait(&cond,&map_mutex);
    pthread_mutex_unlock(&map_mutex);
    for (int i = 0; i < map_num; ++i)
    {
        pthread_create(&reducethread[i], NULL, reduceWorker, this);
        pthread_detach(reducethread[i]);
    }
    
}
Worker::~Worker(){
    pthread_mutex_destroy(&map_mutex);
    pthread_cond_destroy(&cond);
}
void *Worker::mapWorker(void *arg)
{
    Worker *mapworker = (Worker *)arg;
    // 1、初始化client连接用于后续RPC;获取自己唯一的MapTaskID
    buttonrpc mapclient;
    mapclient.as_client("127.0.0.1",5555);
    pthread_mutex_lock(&(mapworker->map_mutex));
    int maptaskid=mapworker->mapid++;
    pthread_mutex_unlock(&(mapworker->map_mutex));
    bool ret=false;
    while(1){
     // 2、通过RPC从Master获取任务
    ret=mapclient.call<bool>("isMapDone").val();
    if(ret){//若map任务全部完成
        pthread_cond_broadcast(&(mapworker->cond));
        return NULL;
    }
    std::string task_status=mapclient.call<std::string>("AssignmapTasks").val();
    if(task_status=="empty")continue;
    printf("%dget task:%s\n",maptaskid,task_status.c_str());
    // 3、拆分任务，任务返回为文件path及map任务编号，将filename及content封装到kv的key及value中
    pthread_mutex_lock(&mapworker->map_mutex);
    char task[task_status.size()+1];
    strcpy(task,task_status.c_str());
    KeyValue kv=mapworker->GetContent(task);
    std::vector<KeyValue> kvs=mapF(kv);
    // 4、执行map函数，然后将中间值写入本地
    mapworker->writeInDisk(kvs,maptaskid);
    // 5、发送RPC给master告知任务已完成
    printf("%d finisf map :%s",maptaskid,task_status.c_str());
    mapclient.call<void>("setMapStat",task_status);
    }

}
int Worker::iHash(std::string task){
    int sum=0;
    for(int i=0;i<task.size();++i){
        sum+=(task[i]-'0');
    }
    sum=sum%reduce_num;
    return sum;
}
void Worker::writeInDisk(const std::vector<KeyValue>&kv,int maptaskidx){
for(const auto &v:kv){
    int reduce_idx=iHash(v.key);
    int fd;
    string path="../myfiles/mr-"+to_string(maptaskidx)+"-"+to_string(reduce_idx);
    int ret=access(path.c_str(),F_OK);
    if(ret==-1){//之前没有创建
        fd=open(path.c_str(),O_WRONLY|O_CREAT|O_APPEND,0664);
    }
    else{
        fd=open(path.c_str(),O_WRONLY,O_APPEND); 
    }
    writeKV(fd,v);
}
}
void Worker::writeKV(int &fd,const KeyValue v){
    string tmp=v.key+",1";
    int p=write(fd,tmp.c_str(),tmp.size());
    if(p==-1){
        perror("write");
        exit(-1);
    }
    close(fd);
}
void Worker::rmFiles()
{
    std::string path;
    for (int i = 0; i < map_num; ++i)
    {
        for (int j = 0; j < reduce_num; ++j)
        {
            std::string tmp = "../myfiles/mr-" + std::to_string(i) + "-" + std::to_string(j);
            int ac = access(path.c_str(), F_OK);
            if (ac == 0)
                remove(path.c_str());
        }
    }
}
void Worker::removeOutputFiles()
{
    std::string path;
    for (int i = 0; i < MAX_REDUCE_NUM; i++)
    {
        path = "../myfiles/mr-out-" + std::to_string(i);
        int ret = access(path.c_str(), F_OK);
        if (ret == 0)
            remove(path.c_str());
    }
}
KeyValue Worker::GetContent(char *filename)
{
    int fd = open(filename, O_RDONLY);
    int length = lseek(fd, 0, SEEK_END); // 偏移量为零，从文件结尾开始算，进而算出总长度
    lseek(fd, 0, SEEK_SET);              // 重新置为文件开始
    KeyValue kv;
    char content[length];
    bzero(content, length);

    int len = read(fd, content, length);
    if (len != length)
    {
        std::cout << "read file:" << filename << std::endl;
        // perror("read");
        exit(-1);
    }
    kv.value = std::string(content);
    kv.key = std::string(filename);
    close(fd);
    return kv;
}