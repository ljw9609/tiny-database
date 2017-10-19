//
//  main.cpp
//  Database_project
//
//  Created by 吕嘉伟 on 16/7/9.
//  Copyright © 2016年 Ljw. All rights reserved.
//

#include <iostream>
#include <string.h>
#include <time.h>
#include <ctime>
#include <map>
#include <vector>
#include "db.h"
using namespace std;


int _db_function_test(char *df){
    char *dat = NULL;
    int s = 0;

    Database tdb;
    Database* db = tdb.db_open(df, 'c');
    
    cout<<"插入num1"<<endl;
    s = db->db_store(db,"num1","value1","insert");
    cout<<"查询num1"<<endl;
    dat = db->db_search(db,"num1");
    cout<<"插入number2"<<endl;
    s = db->db_store(db,"number2","valuevalue2","insert");
    cout<<"插入num3"<<endl;
    s = db->db_store(db,"num3","value3","insert");
    cout<<"修改num1"<<endl;
    s = db->db_store(db,"num1","i hate programming","replace");
    cout<<"查询num1"<<endl;
    dat = db->db_search(db,"num1");
    cout<<"查询number2"<<endl;
    dat = db->db_search(db,"number2");
    cout<<"查询num3"<<endl;
    dat = db->db_search(db,"num3");
    cout<<"删除num1"<<endl;
    s = db->db_delete(db,"num1");
    cout<<"删除number2"<<endl;
    s = db->db_delete(db,"number2");
    cout<<"修改num3"<<endl;
    s = db->db_store(db,"num3","hhh","replace");
    cout<<"插入num1"<<endl;
    s = db->db_store(db,"num1","num1","insert");
    cout<<"插入num2"<<endl;
    s = db->db_store(db,"num2","qwertyuiopa","insert");
    cout<<"查询num1"<<endl;
    dat = db->db_search(db,"num1");
    cout<<"查询num2"<<endl;
    dat = db->db_search(db,"num2");
    cout<<"查询num3"<<endl;
    dat = db->db_search(db,"num3");
    cout<<"查询num4"<<endl;
    dat = db->db_search(db,"num4");
    
    
    return 0;
}

int _db_func_test(char *df){
    char n[100];
    char m[100];
    char *dat = NULL;
    int success = 0;
    int s = 0;
    
    clock_t start, finish;
    double duration;
    start=clock();
    
    Database tdb;
    Database* db = tdb.db_open(df, 'c');

    for(int i = 0;i < 1000000;i++){
        sprintf(n,"key%d",i);
        sprintf(m,"value%d",i);
        s = db->db_store(db, n, m, "insert");
    }
    
    for (int i=0; i<1000000; i++){
        sprintf(n, "key%d", i);
        dat = db->db_search(db, n);
    }
    
    for(int i = 0;i < 500000;i++){
        sprintf(n,"key%d",i);
        sprintf(m,"replace_value%d",i);
        s = db->db_store(db, n, m, "replace");
    }
    
    for(int i = 0;i < 500000;i++){
        sprintf(n,"key%d",i);
        dat = db->db_search(db,n);
    }
    
    
    for(int i = 500000;i < 1000000;i++){
        sprintf(n,"key%d",i);
        s = db->db_delete(db,n);
    }
    
    for(int i = 500000;i < 1000000;i++){
        sprintf(n,"new_key%d",i);
        sprintf(m,"value%d",i);
        s = db->db_store(db, n, m, "insert");
    }
    
    for(int i = 500000;i < 1000000;i++){
        sprintf(n,"new_key%d",i);
        dat = db->db_search(db,n);
    }
    
    
    db->db_close(db);
    return 0;
}


int _db_insert_search_test(int total, char *df){
    char n[10000];
    char m[10000];
    char *dat = NULL;
    int i = 0;
    int success = 0;
    int s = 0;
    int success2 = 0;
    
    clock_t start, finish;
    double duration;
    start=clock();
    
    // 插入
    Database tdb;
    Database* db = tdb.db_open(df, 'c');
    
    for (i=0; i<total; i++){
        
        sprintf(n, "The_key_used_for_test_%d", i);
        sprintf(m,"The_value_used_for_test_%d",i);
        s = db->db_store(db, n, m, "insert");
        if (DB_SUCCESS == s){
            success++;
        }
    }
    
    db->db_close(db);
    
    finish=clock();
    duration=(double)(finish-start)/CLOCKS_PER_SEC;
    
    clock_t start2,finish2;
    double duration2;
    start2 = clock();
    
    // 读取
    Database tdb2;
    Database* db2 = tdb2.db_open(df, 'r');
    
    for (i=0; i<total; i++){
        sprintf(n, "The_key_used_for_test_%d", i);
        dat = db2->db_search(db2, n);
        if (NULL != dat){
            success2++;
        }
    }
    db2->db_close(db2);
    
    finish2 = clock();
    
    duration2=(double)(finish2-start2)/CLOCKS_PER_SEC;
    
    
    printf("数据插入测试———— 成功数: %d, 用时: %fs\n", success, duration);
    printf("数据读取测试———— 成功数: %d, 用时: %fs\n", success2, duration2);
    
    return 0;
}


char *rand_str(char *str,int &len)
{
    int i;
    for(i=0;i<len;++i)
        str[i]='A'+rand()%26;
    str[++i]='\0';
    return str;
}

int naive_test2(int &num,char *df){
    
    map<string,string> maps;
    vector<pair<string,string>> vec;
    srand((unsigned)time(NULL));
    int len = 64;
    int s = 0;
    int success = 0;
    char *dat = NULL;
    char key[len+1];
    char value[len+1];
    string key2;
    string  value2;
    
    clock_t t1, t2;
    double total = 0;
    
    while(maps.size() != num) {
        key2 = rand_str(key, len);
        value2 = rand_str(value, len);
        maps[key2] = value2;
    }
    
    Database tdb;
    Database *db = tdb.db_open(df,'c');
    
    
    map<string,string>::iterator it;
    for(it = maps.begin();it != maps.end();it++){
        //cout<<it->first<<" "<<it->second<<endl;
        vec.push_back(pair<string,string>(it->first,it->second));
    }
    
    
    t1 = clock();
    for(it = maps.begin();it != maps.end();it++){
        //cout<<it->first<<" "<<it->second<<endl;
        s = db->db_store(db,it->first.c_str(),it->second.c_str(),"insert");
    }
    t2 = clock();
    total =(double)(t2 - t1) / CLOCKS_PER_SEC;
    cout<<"随机插入"<<num<<"数据用时 : "<<total<<"秒\n";
    
    
    t1 = clock();
    for(it = maps.begin();it != maps.end();it++){
        dat = db->db_search(db,it->first.c_str());
        if(dat != it->second){
            cout<<"error! not match!\n";
            break;
        }
    }
    t2 = clock();
    total = (double)(t2 - t1)/CLOCKS_PER_SEC;
    cout<<"随机读取"<<num<<"数据用时 : "<<total<<"秒\n";
    
    
    t1 = clock();
    while(success < num/2){
        int val = rand()%num;
        it = maps.find(vec[val].first);
        if(it != maps.end()){
            s = db->db_delete(db,vec[val].first.c_str());
            maps.erase(it);
            success++;
        }
        else continue;
    }
    t2 = clock();
    total = (double)(t2 - t1)/CLOCKS_PER_SEC;
    cout<<"随机删除"<<success<<"数据用时 : "<<total<<"秒\n";
    
    
    db->db_close(db);
    
    

    return 0;
}

int command_line(){
    cout<<"—————— 超级无敌简易版数据库 ———————\n";
    cout<<"1.创建数据库\n"<<"2.打开数据库\n"<<"3.退出\n";
    int cmd;
    int s;
    string file_name;
    string key,value;
    char df[200];
    char* dat = NULL;
    
    while(1){
        cin>>cmd;
        if(cmd == 1){
            cout<<"enter file name : ";
            cin>>file_name;
            string str = "/Users/lvjiawei/Desktop/测试demo/";
            str.append(file_name);
            for(int i = 0;i < 200;i++){
                df[i] =str[i];
            }
            Database tdb;
            Database *db = tdb.db_open(df,'c');
            while(1){
                cout<<"1.插入\n"<<"2.修改\n"<<"3.查找\n"<<"4.删除\n"<<"5.退出\n";
                cin>>cmd;
                if(cmd == 1){
                    cout<<"请输入要插入的key,value对 : ";
                    cin>>key>>value;
                    s = db->db_store(db,key.c_str(),value.c_str(),"insert");
                    if(s == DB_SUCCESS) cout<<"插入成功\n";
                    continue;
                }
                if(cmd == 2){
                    cout<<"请输入要修改的key,value对 : ";
                    cin>>key>>value;
                    s = db->db_store(db,key.c_str(),value.c_str(),"replace");
                    if(s == DB_SUCCESS) cout<<"修改成功\n";
                    continue;
                }
                if(cmd == 3){
                    cout<<"请输入要查找的key : ";
                    cin>>key;
                    dat = db->db_search(db,key.c_str());
                    if(dat != NULL) cout<<"查找成功\n";
                    continue;
                }
                if(cmd == 4){
                    cout<<"请输入要删除的key : ";
                    cin>>key;
                    s = db->db_delete(db,key.c_str());
                    if(s == DB_SUCCESS) cout<<"删除成功\n";
                    continue;
                }
                if(cmd == 5){
                    cout<<"已退出\n";
                    db->db_close(db);
                    exit(0);
                    break;
                }
            }
            
        }
        if(cmd == 2){
            cout<<"enter file name : ";
            cin>>file_name;
            string str = "/Users/lvjiawei/Desktop/";
            str.append(file_name);
            for(int i = 0;i < 200;i++){
                df[i] =str[i];
            }
            Database tdb;
            Database *db = tdb.db_open(df,'w');
            while(1){
                cout<<"1.插入\n"<<"2.修改\n"<<"3.查找\n"<<"4.删除\n"<<"5.退出\n";
                cin>>cmd;
                if(cmd == 1){
                    cout<<"请输入要插入的key,value对 : ";
                    cin>>key>>value;
                    s = db->db_store(db,key.c_str(),value.c_str(),"insert");
                    if(s == DB_SUCCESS) cout<<"插入成功\n";
                    continue;
                }
                if(cmd == 2){
                    cout<<"请输入要修改的key,value对 : ";
                    cin>>key>>value;
                    s = db->db_store(db,key.c_str(),value.c_str(),"replace");
                    if(s == DB_SUCCESS) cout<<"修改成功\n";
                    continue;
                }
                if(cmd == 3){
                    cout<<"请输入要查找的key : ";
                    cin>>key;
                    dat = db->db_search(db,key.c_str());
                    if(dat != NULL) cout<<"查找成功\n";
                    continue;
                }
                if(cmd == 4){
                    cout<<"请输入要删除的key : ";
                    cin>>key;
                    s = db->db_delete(db,key.c_str());
                    if(s == DB_SUCCESS) cout<<"删除成功\n";
                    continue;
                }
                if(cmd == 5){
                    cout<<"已退出\n";
                    db->db_close(db);
                    exit(0);
                    break;
                }
            }
        }
        if(cmd == 3){
            exit(0);
            break;
        }
    }
    
    return 0;
}





/*
 * 测试程序
 */
int main(){
    command_line();
    
    
    
    
    /*
     // 增删改查功能测试
     cout<<"========= 超级无敌简易版数据库 =========\n";
     cout<<"============= 功能测试 ===========\n";
     string str = "/Users/lvjiawei/Desktop/database_test1";
     char df[200];
     for(int i = 0;i < 200;i++){
     df[i] =str[i];
     }
     _db_function_test(df);
    */
    
    
    /*
    // 读写性能测试
    cout<<("============= 性能测试 ===========\n");
    string str = "/Users/lvjiawei/Desktop/database_test3";
    char df[200];
    for(int i = 0;i < 200;i++){
        df[i] =str[i];
    }
    int total = 1000000;
    
    _db_insert_search_test(total, df);
    */
    
    
    /*
    cout<<("============= 功能测试 ===========\n");
    string str = "/Users/lvjiawei/Desktop/database_test2";
    char df[200];
    for(int i = 0;i < 200;i++){
        df[i] =str[i];
    }
    _db_func_test(df);
    */
    
    /*
    cout<<"============= 性能测试（随机字符串增删查改）=============\n";
    string str = "/Users/lvjiawei/Desktop/database_test2";
    char df[200];
    for(int i = 0;i < 200;i++){
        df[i] =str[i];
    }
    int total;
    cout<<"输入测试的数据量 : ";
    cin>>total;
    naive_test2(total,df);
    */
    return 0;
    
}

