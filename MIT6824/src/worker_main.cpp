#include"worker.h"

int main(){
   
    buttonrpc client;
    client.as_client("127.0.0.1",5555);
    int map_num=client.call<int>("getMapNum").val();
    int reduce_num=client.call<int>("getReduceNum").val();
    Worker wk(map_num,reduce_num);
    pthread_mutex_init(&wk.map_mutex,NULL);
    pthread_cond_init(&wk.cond,NULL);
    //清理上次输出的中间文件
    //wk.rmFiles();
    //清理上次输出的结果文件
    wk.removeOutputFiles();
    //创建线程
    wk.CreateThread();
    //检查是否结束
    while(1){
        if(client.call<bool>("isMapDone").val())
        break;
        sleep(1);
    }
    //清理中间文件
    wk.rmFiles();
    //释放内存
   
}