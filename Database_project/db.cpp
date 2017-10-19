//
//  main.cpp
//  Database_project
//
//  Created by 吕嘉伟 on 16/7/9.
//  Copyright © 2016年 Ljw. All rights reserved.
//

#include "db.h"
#include <iostream>
using namespace std;

// 消除右边的空格
char * rtrim(char *str){
    int p;
    p = strlen(str) - 1;
    while ( isspace(str[p]) )
        p--;
    str[p + 1] = '\0';
    return str;
}



/*
 * 打印数据
 */
void Database::print_data_record(struct data_record *data_record){
    cout<<"\n--------------数据记录--------------\n";
    cout<<"数据长度 : "<<data_record->len<<endl;
    cout<<"数据内容 : "<<data_record->data<<endl;
}

/*
 * 打印键值
 */
void Database::print_key_record(struct key_record *key_record){
    cout<<"\n--------------键值记录--------------\n";
    cout<<"键值内容 : "<<key_record->key<<endl;
    cout<<"对应数据在文件中的偏移量 : "<<key_record->data_ptr<<endl<<endl;
}

/*
 *打开文件
 */
Database* Database::db_open(const char*path,char mode){
    Database* db = NULL;
    
    //检查打开的方式
    if (mode != 'r' && mode != 'c' && mode != 'w'){
        error("打开文件错误");
        return NULL;
    }
    
    int len = strlen(path);
    if ( NULL == (db = _db_alloc(len))){    //调用_db_alloc函数为db分配空间
        error("内存分配错误");
        return NULL;
    }
    
    // 初始化文件名，分别为xxx.idx和xxx.dat
    strcpy(db->idx_name, path);
    strcat(db->idx_name, ".idx");
    strcpy(db->dat_name, path);
    strcat(db->dat_name, ".dat");
    
    // 检查文件是否已存在
    int exists = 0;
    FILE *fp = fopen(db->idx_name,"rb");
    if (fp == NULL){
        exists = -1;
    }
    // 如果文件不存在或做"c"(create)操作,则创建文件
    if ( mode == 'c'|| exists ){
        if ( NULL == (db->idx_fp = fopen(db->idx_name, "wb+"))){
            error("打开索引文件错误");
            return NULL;
        }
        if ( NULL == (db->dat_fp = fopen(db->dat_name, "wb+")) ){
            error("打开数据文件错误");
            return NULL;
        }
        if ( DB_OK != _db_create(db) ){
            error("创建数据库错误");
            return NULL;
        }
    }
    // "r",只读,"rb"二进制读取
    else if ( mode == 'r' ){
        if ( NULL == (db->idx_fp = fopen(db->idx_name, "rb")) ){
            error("打开索引文件错误");
            return NULL;
        }
        if ( NULL == (db->dat_fp = fopen(db->dat_name, "rb")) ){
            error("打开数据文件错误");
            return NULL;
        }
    }
    // "w",读写,"rb+"二进制读写
    else if ( mode == 'w' ){
        if ( NULL == (db->idx_fp = fopen(db->idx_name, "rb+")) ){
            error("打开索引文件错误");
            return NULL;
        }
        if ( NULL == (db->dat_fp = fopen(db->dat_name, "rb+")) ){
            error("打开数据文件错误");
            return NULL;
        }
    }
    
    return db;
    
}

/*
 * 分配空间
 */
Database* Database::_db_alloc(int namelen){
    Database* db = NULL;
    /*
     * 为database分配内存空间
     */
    if ( NULL == (db = static_cast<Database*>(malloc(sizeof(Database)))) ) {
        error("内存分配错误");
        db->idx_fp = db->dat_fp = NULL;
        return NULL;
    }
    
    /*
     * 为文件名分配内存空间
     * +5 是因为 ".idx" or ".dat" 和末尾的'\0'
     */
    if ( NULL == (db->idx_name = static_cast<char*>(calloc(namelen + 5, sizeof(char)))) )
        error("内存分配错误");
    if ( NULL == (db->dat_name = static_cast<char*>(calloc(namelen + 5, sizeof(char)))) )
        error("内存分配错误");
    
    // 哈希表大小
    db->nhash = MAX_HASH_BUCKET;
    
    // 初始时偏移量均为0
    db->next_off = db->idxoff = db->preidx = db->key_ptr_pos = 0;
    
    return db;
}



/*
 * 清除Database结构，释放所有分配的空间，关闭文件
 */
void Database::db_close(Database* db){
    if ( NULL == db ){
        return;
    }
    if ( NULL != db->idx_fp )
        fclose(db->idx_fp);
    if ( NULL != db->dat_fp )
        fclose(db->dat_fp);
    if ( NULL != db->idx_name ){
        free(db->idx_name);
        db->idx_name = NULL;
    }
    if ( NULL != db->dat_name ){
        free(db->dat_name);
        db->dat_name = NULL;
    }
    free(db);
    db = NULL;
}

/*
 * 创建并初始化database
 *
 */
RC Database::_db_create(Database *db) {
    if (!db){
        return DB_ERROR;
    }
    /*
     * 初始化索引文件
     */
    fseek(db->idx_fp,0,SEEK_SET);   //指针先指向文件开头
    int zero = 0;
    fwrite(&zero,INT_SIZE,1,db->idx_fp);
    for(off_t offset = (INT_SIZE + 1*PTR_SIZE - INT_SIZE);offset <= INT_SIZE + (MAX_HASH_BUCKET*PTR_SIZE) - INT_SIZE;offset += INT_SIZE){
        fseek(db->idx_fp, offset, SEEK_SET);
        fwrite(&zero, INT_SIZE, 1, db->idx_fp);
    }
    fflush(db->idx_fp);
    
    /*
     * 初始化数据文件
     */
    fseek(db->dat_fp,0,SEEK_SET);
    fwrite(&zero,INT_SIZE,1,db->dat_fp);
    fflush(db->dat_fp);
    
    return DB_OK;
}

/*
 * 哈希函数 注：采用了BKDR哈希算法，一种优秀的字符串hash函数
 */
DBHASH Database::_db_hash(Database *db, const char *str) {
    DBHASH seed = 131; // magic number，可用31,131,1313,13131等等
    DBHASH hash = 0;
    while (*str) {
        hash = hash * seed + (*str++);
    }
    return (hash) % db->nhash;
}


/*
 * 在索引中找到键值
 */
RC Database::_db_find_key(Database *db, const char *key) {
    int key_pos = 0;        //当前键值在哈希桶中的位置
    int key_ptr_pos = 0;    //键值所指向的位置
    int key_s_length = 0;
    
    struct key_record *key_record = NULL;
    
    db->hash = _db_hash(db, key);
    
    // 找到键值的位置
    key_pos = INT_SIZE + (db->hash * PTR_SIZE);
    key_s_length = sizeof(struct key_record);
    fseek(db->idx_fp, key_pos, SEEK_SET);
    if ( fread(&key_ptr_pos, PTR_SIZE, 1, db->idx_fp) != 1)
        return KEY_ERROR;
    
    db->key_ptr_pos = key_ptr_pos;
    if (key_ptr_pos == 0) {
        db->idxoff = 0;
        return KEY_FIRST;
    }
    
    if ( (key_record = static_cast<struct key_record*>(malloc(key_s_length)) )== NULL) {
        error("内存分配错误");
        free(key_record);
        key_record = NULL;
        return KEY_ERROR;
    }
    
    int find = 0;
    while (true) { //循环，直到找到为止或不存在退出
        // 键值不存在
        fseek(db->idx_fp, key_ptr_pos, SEEK_SET);
        fread(key_record, key_s_length, 1, db->idx_fp);
        db->preidx = ftell(db->idx_fp); //同哈希值前一键值末尾位置
        if ( NULL == key_record || NULL == key_record->key ){
            db->idxoff = key_ptr_pos;
            free(key_record);
            key_record = NULL;
            return KEY_ERROR;
        }
        // 比较是否是要找的那个键值 rtrim用来消除空格
        if ( strcmp(rtrim(key_record->key), key) == 0){
            find = 1;
            break;
        }
        // 键值位置指针指向同哈希值的下一个
        if ( 0 == key_record->next_ptr ){
            break;
        }
        key_ptr_pos = key_record->next_ptr;
    }
    
    // 保存键值的位置
    if ( !find ) {
        db->idxoff = key_ptr_pos;
        free(key_record);
        key_record = NULL;
        return KEY_NOTEXIST;
    }
    db->idxoff = key_ptr_pos;
    db->datoff = key_record->data_ptr;
    
    return KEY_EXIST;
}

/*
 * 将数据写入文件
 */
RC Database::_db_write_data_to_file(Database *db, struct data_record *data_record){
    //fwrite(&(data_record->flag), sizeof(data_record->flag), 1, db->dat_fp);
    fwrite(&(data_record->len), sizeof(data_record->len), 1, db->dat_fp);
    fwrite(data_record->data, data_record->len, 1, db->dat_fp);
    fwrite(&(data_record->next), sizeof(data_record->next), 1, db->dat_fp);
    
    return DB_SUCCESS;
}

RC Database::_db_write_data(Database* db,const char* key,const char*value){
    /*
     * 将数据保存入data结构
     */
    struct data_record *data_record = NULL;
    data_record = static_cast<struct data_record*>(malloc(sizeof(struct data_record)));
    data_record->len  = strlen(value);
    data_record->data = static_cast<char *>(malloc(data_record->len+1)); //末尾'\0'
    strcpy(data_record->data, value);
    data_record->data[data_record->len+1] = '\0';
    data_record->next = 0;
    
    if (TEST){
        print_data_record(data_record);
    }
    
    // 写入数据
    int record_pos = 0;
    int data_s_len = 0;
    int free_ptr = 0;
    
    fseek(db->dat_fp,0,SEEK_SET);
    fread(&free_ptr,INT_SIZE,1,db->dat_fp);
    
    fseek(db->dat_fp, 0, SEEK_END);
    record_pos = ftell(db->dat_fp);
    data_s_len = sizeof(struct data_record);
    
    // 如果是第一个数据
    if ( record_pos == INT_SIZE ){
        _db_write_data_to_file(db, data_record);
        if (TEST){
            printf("写入数据的键值:%s\n", key);
        }
    }
    else if(free_ptr == 0){
        fseek(db->dat_fp,0,SEEK_END);
        record_pos = ftell(db->dat_fp);
        _db_write_data_to_file(db,data_record);
        if(TEST) printf("写入数据的键值:%s\n",key);
    }
    else{
        record_pos = free_ptr;
        fseek(db->dat_fp,free_ptr,SEEK_SET);
        int len = 0;
        int temp_off = record_pos;
        int cur_off = 0;
        fread(&len,INT_SIZE,1,db->dat_fp);
        
        while(true){
            if(len == data_record->len){
                fseek(db->dat_fp,temp_off+INT_SIZE,SEEK_SET);
                fwrite(value,len,1,db->dat_fp);
                int temp_off_2 = 0;
                fread(&temp_off_2,INT_SIZE,1,db->dat_fp);
                fseek(db->dat_fp,cur_off,SEEK_SET);
                fwrite(&temp_off_2,INT_SIZE,1,db->dat_fp);
                break;
            }
            else{
                fseek(db->dat_fp,len,SEEK_CUR);
                cur_off = ftell(db->dat_fp);
                fread(&temp_off,INT_SIZE,1,db->dat_fp);
                if(temp_off == 0) {
                    fseek(db->dat_fp, 0, SEEK_END);
                    record_pos = ftell(db->dat_fp);
                    _db_write_data_to_file(db, data_record);
                    break;
                }
                else{
                    record_pos = temp_off;
                    fseek(db->dat_fp,temp_off,SEEK_SET);
                    fread(&len,INT_SIZE,1,db->dat_fp);
                }
            }
        }
        
        
    }
    fflush(db->dat_fp); //清楚缓冲区
    free(data_record);
    data_record = NULL;
    return record_pos;
}

RC Database::_db_write_index(Database* db,const char *key,const char *value,int record_pos){
    RC rc = _db_find_key(db, key);
    
    struct key_record *key_record = NULL;
    int key_pos = 0;
    int key_ptr_pos = 0;
    int key_s_len = 0;   //键值结构长度
    int	key_len = 0;    //键值长度
    
    // 键值记录
    key_record = static_cast<struct key_record*>(malloc(sizeof(struct key_record)));
    key_s_len = sizeof(struct key_record);
    key_len  = strlen(key);
    //key_record->flag = FLAG_NOR;
    memset(key_record->key, 0, MAX_KEY_LEN);    //先将结构中key值初始化
    memcpy(key_record->key, key, key_len);     //将键值赋值入结构中key
    key_record->data_ptr = record_pos;
    
    if (TEST){
        print_key_record(key_record);
    }
    
    // 键值不存在
    if (KEY_FIRST == rc || KEY_NOTEXIST == rc){
        // 写入键值
        key_record->next_ptr = 0;
        
        fseek(db->idx_fp,0,SEEK_SET);
        int free_ptr = 0;
        fread(&free_ptr,INT_SIZE,1,db->idx_fp);
        if(free_ptr != 0){
            key_ptr_pos = free_ptr;
            fseek(db->idx_fp,free_ptr,SEEK_SET);
            fseek(db->idx_fp,MAX_KEY_LEN+PTR_SIZE,SEEK_CUR);
            int temp_off = 0;
            fread(&temp_off,INT_SIZE,1,db->idx_fp);
            fseek(db->idx_fp,free_ptr,SEEK_SET);
            fwrite(key_record, key_s_len,1,db->idx_fp);
            fseek(db->idx_fp,0,SEEK_SET);
            fwrite(&temp_off,INT_SIZE,1,db->idx_fp);
        }
        else{
            fseek(db->idx_fp, 0, SEEK_END);
            key_ptr_pos = ftell(db->idx_fp);
            fwrite(key_record, key_s_len, 1, db->idx_fp);
        }
        
        // 如果哈希值还不存在，则定位到哈希桶中对应位置
        if (KEY_FIRST == rc){
            key_pos = INT_SIZE + (db->hash * PTR_SIZE);
            fseek(db->idx_fp, key_pos, SEEK_SET);
            if (TEST){
                printf("写入未出现的数据，且为该哈希值下第一个键值，键值为:%s\n", key);
            }
        }
        // 如果哈希值存在但键值不存在，则定位到前一个存在的键值之后
        else if (KEY_NOTEXIST == rc){
            fseek(db->idx_fp, (db->preidx - PTR_SIZE), SEEK_SET);
            if (TEST){
                printf("写入未出现的数据，且该哈希值已出现，键值为:%s\n", key);
            }
        }
        fwrite(&key_ptr_pos, PTR_SIZE, 1, db->idx_fp);
        fflush(db->idx_fp);
        
        free(key_record);
        key_record = NULL;
        return DB_SUCCESS;
    }
    // 如果键值已存在，则替换
    if (KEY_EXIST == rc){
        // 定位到键值结构中指向数据的指针
        fseek(db->idx_fp, (db->idxoff + MAX_KEY_LEN), SEEK_SET);
        // 将其修改为新的数据的指针
        fwrite(&record_pos, PTR_SIZE, 1, db->idx_fp);
        
        fflush(db->idx_fp);
        
        if (TEST){
            printf("修改已存在的数据，键值为:%s\n", key);
        }
        free(key_record);
        key_record = NULL;
        
        /*
         * 处理数据文件
         */
        int zero = 0;
        int free_ptr = 0;
        fseek(db->dat_fp,(db->datoff),SEEK_SET);
        int len = 0;
        fread(&len,INT_SIZE,1,db->dat_fp);
        fseek(db->dat_fp,len,SEEK_CUR);
        fwrite(&zero,INT_SIZE,1,db->dat_fp);
        
        int dat_off = db->datoff;
        fseek(db->dat_fp,0,SEEK_SET);
        fread(&free_ptr,INT_SIZE,1,db->dat_fp);
        if(free_ptr == 0){
            free_ptr = dat_off;
            fseek(db->dat_fp,0,SEEK_SET);
            fwrite(&free_ptr,INT_SIZE,1,db->dat_fp);
            if(TEST) cout<<"第一个空闲数据偏移量 : "<<free_ptr<<endl;
        }
        else{
            fseek(db->dat_fp,0,SEEK_SET);
            fwrite(&dat_off,INT_SIZE,1,db->dat_fp);
            int temp_len = 0;
            fseek(db->dat_fp,dat_off,SEEK_SET);
            fread(&temp_len,INT_SIZE,1,db->dat_fp);
            fseek(db->dat_fp,temp_len,SEEK_CUR);
            fwrite(&free_ptr,INT_SIZE,1,db->dat_fp);
        }
        fflush(db->dat_fp);
        
        
        return DB_SUCCESS;
    }
    
    return DB_OK;
}

/*
 * 数据操作，插入，修改
 */
RC	Database::db_store(Database *db, const char *key, const char *value, string mode){
    // 检查参数
    if ( NULL == db || NULL == key || NULL == value ){
        return DB_ERROR;
    }
    if ( mode != "INSERT" && mode != "REPLACE" && mode != "insert" && mode != "replace"){
        return DB_ERROR;
    }
    
    unsigned long key_length = strlen(key);
    unsigned long data_length = strlen(value);
    
    if ( key_length <= 0 || key_length > MAX_KEY_LEN){
        return DB_ERROR;
    }
    if ( data_length > MAX_RECORD_LEN){
        return DB_ERROR;
    }
    
    // 找到键值
    RC rc = _db_find_key(db, key);
    if (TEST){
        printf("找到该键值为:%s  状态为:%d\n", key, rc);
    }
    if (rc == KEY_ERROR){
        return DB_ERROR;
    }
    
    // 若键值已存在，还执行插入操作
    if ((mode == "INSERT" ||mode == "insert" )&& KEY_EXIST == rc){
        error("插入错误");
    }
    // 若键值不存在，还执行修改操作
    if ((mode == "REPLACE" || mode == "replace")&& (rc == KEY_FIRST||rc == KEY_NOTEXIST)){
        error("替换错误");
    }
    
    int record_pos = _db_write_data(db,key,value);
    return _db_write_index(db,key,value,record_pos);
}


void Database::_db_delete(Database *db){
    /*
     * 处理索引文件
     */
    char space[MAX_KEY_LEN];
    int zero = 0;
    memset(space,0,MAX_KEY_LEN);
    fseek(db->idx_fp,db->idxoff,SEEK_SET);
    fwrite((char*)&space,1,1,db->idx_fp);
    fwrite(&zero,INT_SIZE,1,db->idx_fp);
    fwrite(&zero,INT_SIZE,1,db->idx_fp);
    
    int offset = db->idxoff;
    fseek(db->idx_fp,0,SEEK_SET);
    int free_ptr = 0;
    fread(&free_ptr,INT_SIZE,1,db->idx_fp);
    
    fseek(db->idx_fp,0,SEEK_SET);
    if(free_ptr == 0){
        free_ptr = offset;
        fwrite(&free_ptr,INT_SIZE,1,db->idx_fp);
        if(TEST) cout<<"第一个空间索引偏移量 : "<<free_ptr<<endl;
    }
    else{
        fseek(db->idx_fp,0,SEEK_SET);
        fwrite(&offset,INT_SIZE,1,db->idx_fp);
        fseek(db->idx_fp,offset+MAX_KEY_LEN+PTR_SIZE,SEEK_SET);
        fwrite(&free_ptr,INT_SIZE,1,db->idx_fp);
    }
    
    fflush(db->idx_fp);
    /*
     * 处理数据文件
     */
    fseek(db->dat_fp,(db->datoff),SEEK_SET);
    int len = 0;
    fread(&len,INT_SIZE,1,db->dat_fp);
    fseek(db->dat_fp,len,SEEK_CUR);
    fwrite(&zero,INT_SIZE,1,db->dat_fp);
    
    int dat_off = db->datoff;
    fseek(db->dat_fp,0,SEEK_SET);
    fread(&free_ptr,INT_SIZE,1,db->dat_fp);
    if(free_ptr == 0){
        free_ptr = dat_off;
        fseek(db->dat_fp,0,SEEK_SET);
        fwrite(&free_ptr,INT_SIZE,1,db->dat_fp);
        if(TEST) cout<<"第一个空闲数据偏移量 : "<<free_ptr<<endl;
    }
    else{
        fseek(db->dat_fp,0,SEEK_SET);
        fwrite(&dat_off,INT_SIZE,1,db->dat_fp);
        int temp_len = 0;
        fseek(db->dat_fp,dat_off,SEEK_SET);
        fread(&temp_len,INT_SIZE,1,db->dat_fp);
        fseek(db->dat_fp,temp_len,SEEK_CUR);
        fwrite(&free_ptr,INT_SIZE,1,db->dat_fp);
    }
    fflush(db->dat_fp);
}

/*
 * 删除数据
 */
RC	Database::db_delete(Database *db, const char *key){
    // 检查参数
    if (NULL == db || NULL == key){
        return DB_ERROR;
    }
    int keylen = 0;
    keylen = strlen(key);
    if ( keylen <= 0 || keylen > MAX_KEY_LEN){
        return DB_ERROR;
    }
    
    // 找到键值
    RC rc = 0;
    rc = _db_find_key(db, key);
    
    if (TEST){
        printf("找到键值:%s  状态为:%d\n", key, rc);
    }
    if (KEY_ERROR == rc){
        return DB_ERROR;
    }
    if (KEY_FIRST == rc || KEY_NOTEXIST == rc){
        return DB_OK;
    }
    

    int key_next_ptr = 0;
    
    // 如果该键值是该哈希值的第一个键值
    if (KEY_EXIST == rc && db->key_ptr_pos == db->idxoff){
        
        
        fseek(db->idx_fp, db->idxoff, SEEK_SET);
        // 指向该哈希值的下一个键值位置，将其更新为该哈希值下的第一个键值
        fseek(db->idx_fp, (MAX_KEY_LEN+PTR_SIZE), SEEK_CUR);
        fread(&key_next_ptr, PTR_SIZE, 1, db->idx_fp);
        
        int key_pos = 0;
        key_pos = INT_SIZE + (db->hash * PTR_SIZE);
        fseek(db->idx_fp, key_pos, SEEK_SET);
        fwrite(&key_next_ptr, INT_SIZE, 1, db->idx_fp);
        
        _db_delete(db);
        fflush(db->idx_fp);
        
        if (TEST){
            printf("删除已存在的键值:%s \n", key);
        }
        
        return DB_SUCCESS;
    }
    
    // 如果该键值不是该哈希值的第一个键值
    if ( KEY_EXIST == rc ){
        
        fseek(db->idx_fp, db->idxoff, SEEK_SET);
        // 指向下一个键值位置，将其更新为上上个键值的下一个键值
        fseek(db->idx_fp, (MAX_KEY_LEN+PTR_SIZE), SEEK_CUR);
        fread(&key_next_ptr, PTR_SIZE, 1, db->idx_fp);
        
        fseek(db->idx_fp, (db->preidx+MAX_KEY_LEN+PTR_SIZE), SEEK_SET);
        fwrite(&key_next_ptr, PTR_SIZE, 1, db->idx_fp);
        
        _db_delete(db);
        
        fflush(db->idx_fp);
        
        if (TEST){
            printf("删除已存在的键值:%s \n", key);
        }
        
        return DB_SUCCESS;
    }
    return DB_OK;
}

/*
 * 搜索数据
 */
char *Database::db_search(Database *db, const char *key){
    // 检查元素
    if ( NULL == db || NULL == key ){
        return NULL;
    }
    int keylen = 0;
    keylen = strlen(key);
    if ( keylen <= 0 || keylen > MAX_KEY_LEN){
        return NULL;
    }
    
    // 查找键值不存在或出现错误
    RC rc = 0;
    rc = _db_find_key(db, key);
    if (TEST){
        printf("找到键值:%s  状态为:%d\n", key, rc);
    }
    if ( KEY_ERROR == rc ){
        return NULL;
    }
    if ( KEY_FIRST == rc || KEY_NOTEXIST == rc ){
        cout<<"不存在"<<endl;
        return NULL;
    }
    
    // 找到键值，读取数据
    if ( KEY_EXIST == rc ){
        int len = 0, dat_len = 0;
        char *dat_buf = NULL;
        char *ret_buf = NULL;
        
        dat_buf = static_cast<char*>(calloc(MAX_RECORD_LEN, 1));    //分配空间
        
        fseek(db->dat_fp, (db->datoff), SEEK_SET);
        fread(&len, PTR_SIZE, 1, db->dat_fp);   //得到数据的长度
        fread(dat_buf, sizeof(char), len, db->dat_fp);  //读入buffer
        
        dat_len = strlen(dat_buf);
        ret_buf = static_cast<char*>(calloc(dat_len+1, 1)); //末尾'\0'
        memcpy(ret_buf, dat_buf, dat_len+1);
        free(dat_buf);
        dat_buf = NULL;
        if(TEST){
            printf("输出该数据:%s\n",ret_buf);
        }
        return (char *)ret_buf;
    }
    
    return NULL;
}


Database::Database(){}

Database::~Database(){}



