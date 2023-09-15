#include"master.h"
int main(int argc,char*argv[]){
    if(argc < 2){
        cout<<"missing parameter! The format is ./Master pg*.txt"<<endl;
        exit(-1);
    }
    // alarm(10);
    buttonrpc server;
    server.as_server(5555);
    Master master(13, 9);
    master.GetFile(argv, argc);
    server.bind("getMapNum", &Master::GetMapNum, &master);
    server.bind("getReduceNum", &Master::GetReduceNum, &master);
    server.bind("AssignmapTasks", &Master::AssignmapTasks, &master);
    server.bind("setMapStat", &Master::setMapStat, &master);
    server.bind("isMapDone", &Master::isMapDone, &master);
    server.bind("AssignReduceTask", &Master::AssignreduceTasks, &master);
    server.bind("setReduceStat", &Master::setReduceStat, &master);
    server.bind("isReduceDone", &Master::isReduceDone, &master);
    server.run();
    return 0;
}