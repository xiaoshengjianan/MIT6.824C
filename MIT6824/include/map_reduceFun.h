
#ifndef MAP_REDUCEFUN_H
#define MAP_REDUCEFUN_H
#include<iostream>
#include<string>
#include<vector>
#include<cstring>
#include<stdlib.h>
class KeyValue{
    public:
    std::string key;
    std::string value;
};
std::vector<std::string> split(char*text,int length){
std::vector<std::string>str;
std::string tmp;
for(int i=0;i<length;++i){
    if((text[i]>='a'&&text[i]<='z')||(text[i]>='A'&&text[i]<='Z')){
        tmp+=text[i];
    }else{
        if(tmp.size()!=0)str.push_back(tmp);
        tmp="";
    }
    if(tmp.size()!=0)str.push_back(tmp);
    return str;
}
}
std::vector<KeyValue>mapF(KeyValue kv){
    int length=kv.value.size();
    std::vector<KeyValue>kvs;
    char content[length+1];
    std::strcpy(content,(kv.value).c_str());
    std::vector<std::string>str=split(content,length);
    for(const auto& s:content){
        KeyValue tmp;
        tmp.key=s;
        tmp.value="1";
        kvs.emplace_back(tmp);
    }
    return kvs;
}

#endif