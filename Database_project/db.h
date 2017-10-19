//
//  main.cpp
//  Database_project
//
//  Created by 吕嘉伟 on 16/7/9.
//  Copyright © 2016年 Ljw. All rights reserved.
//

#ifndef _DB_H_
#define _DB_H_

#include <sys/types.h>
#include <sys/file.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include "error.h"

#define	TEST		0

/* 返回值定义 */
#define DB_SUCCESS		1
#define DB_OK			0
#define DB_ERROR		-1

/* 键值查询定义 */
#define KEY_FIRST		1		/* 主键对应的哈希值不存在，主键也不存在 */
#define KEY_NOTEXIST	2		/* 主键对应的哈希值存在，主键不存在 */
#define KEY_EXIST		3		/* 主键存在 */
#define KEY_ERROR		-1		/* 查找主键发生错误 */

#define INT_SIZE			4			/* 整数大小 */
#define PTR_SIZE			4			/* 指针大小 */

#define MAX_HASH_BUCKET		999983		/* 最大哈希桶大小 */
#define MAX_KEY_LEN			128			/* 键值最大长度 */
#define MAX_RECORD_LEN		128         /* 数据最大长度 */

#define BUFFER_SIZE				1048576        /* 默认数据缓存大小 */
#define MAX_BUF_SIZE			1048576		   /* 键值和数据的最大大小 */

struct key_record {
    char key[MAX_KEY_LEN];                  /* 键值 */
    int data_ptr;							/* 指到数据文件中具体value的位置 */
    int next_ptr;							/* 指到同一个哈希值下一个键值的位置 */
};

struct data_record {
    int len;								/* 表示数据长度 */
    char *data;								/* 数据 */
    int next;								/* 下一条数据的指针 */
};

typedef unsigned long DBHASH;  /* 哈希值 */
typedef int RC;                /* return code,用来表示状态 */

class Database{
public:
    Database();  /* 构造函数 */
    ~Database(); /* 析构函数 */
    Database* db_open(const char*,char);                            /* 打开数据库 */
    void db_close(Database*);                                       /* 关闭数据库 */
    char *db_search(Database* ,const char*);                        /* 搜索数据 */
    RC db_store(Database* ,const char*,const char*,std::string);    /* 储存数据，插入、替换 */
    RC db_delete(Database* ,const char*);                           /* 删除数据 */
    void print_data_record(struct data_record *);                   /* 打印数据 */
    void print_key_record(struct key_record *);                     /* 打印键值 */
    
    FILE *idx_fp;				/* 索引文件指针 */
    FILE *dat_fp;				/* 数据文件指针 */
    
    char  *idx_name;			/* 索引文件名 */
    char  *dat_name;			/* 数据文件名*/
    
    DBHASH nhash;				/* 散列表大小 */
    DBHASH hash;				/* 当前哈希值 */
    
    long long int  idxoff;              /* 哈希表中键值的偏移量 */
    long long int key_ptr_pos;			/* 哈希表中同一哈希值第一个键值的位置 */
    long long int next_off;				/* 同一哈希值下一个数据索引的偏移量 */
    long long int preidx;				/* 前一个键值位置 */
    long long int  datoff;				/* 数据文件中数据的偏移量 */
    
    
private:
    
    Database* _db_alloc(int);
    RC _db_create(Database*);
    DBHASH _db_hash(Database*,const char*);
    RC _db_find_key(Database*,const char*);
    RC _db_write_data_to_file(Database*,struct data_record*);
    RC _db_write_data(Database*,const char*,const char*);
    RC _db_write_index(Database*,const char*,const char*,int);
    void _db_delete(Database*);
    
};



#endif
















