#include <sqlite3.h>
#include <linux/types.h>
#include <limits.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include "uthash.h"
#include <inttypes.h>

#define PREP_SQL(QUERY) res = sqlite3_prepare_v2(db, QUERY, -1, &pstmt, NULL); \
    			if (res != SQLITE_OK) { \
      				fprintf(stderr, "SQL prep error: %s\n", sqlite3_errmsg(db)); \
      				sqlite3_close(db); \
      				db = NULL; \
      				return -res; \
    			}
    			
#define EXEC_SQL    	res = sqlite3_step(pstmt); \
    			if (res != SQLITE_DONE) { \
      				fprintf(stderr, "SQL exec error: %s\n", sqlite3_errmsg(db)); \
      				sqlite3_finalize(pstmt); \
      				sqlite3_close(db); \
      				db = NULL; \
      				return -res; \
    			} \
    			sqlite3_finalize(pstmt);

sqlite3 *db;

static int callback(void *NotUsed, int argc, char **argv, char **azColName) 
{ /* debug callback */
   int i;
   for(i = 0; i<argc; i++) {
      printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
   }
   printf("\n");
   return 0;
}

int create_tables()
{
	char* sql1 = 	"CREATE TABLE MappingTable ( \
    			 key BLOB, \
    			 path TEXT NOT NULL UNIQUE, \
    			 PRIMARY KEY(key) \
			 );";

	char* sql2 = 	"CREATE INDEX idx_key ON MappingTable (key); \
			 CREATE INDEX idx_path ON MappingTable (path);";
			
	int res;
	char* err_msg = NULL;
	res = sqlite3_exec(db, sql1, callback, 0, &err_msg);
	if(res != SQLITE_OK) {
      		fprintf(stderr, "SQL error: %s\n", err_msg);
      		sqlite3_free(err_msg);
      		sqlite3_close(db);
      		db = NULL;
      		return -res;
   	} 
   	res = sqlite3_exec(db, sql2, callback, 0, &err_msg);
   	if(res != SQLITE_OK) {
      		fprintf(stderr, "SQL error: %s\n", err_msg);
      		sqlite3_free(err_msg);
      		sqlite3_close(db);
      		db = NULL;
      		return -res;
   	} 
   	return 0;
}

int create_db()
{
	int res;
	res = sqlite3_open("keys.db", &db);
	if( res ) {
      		fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
      		sqlite3_close(db);
      		db = NULL;
      		return -res;
      	}
      	return create_tables();
}

int open_db()
{
	int res;
	res = sqlite3_open_v2("keys.db", &db, SQLITE_OPEN_READWRITE, NULL);
	if( res ) {
      		return create_db();
      	}
      	return 0;
}

void close_db()
{
	sqlite3_close_v2(db);
	db = NULL;
}

int generate_key(char* path, void* ptr)
{
	char* sql1 = 	"SELECT key FROM MappingTable \
			 WHERE path = ?1";
	char* sql2 =	"INSERT INTO MappingTable \
			 VALUES(?1, ?2)";
	int res, len = strlen(path), i;
	FILE* fout;
	const void* blob;
	char* new_key = NULL;
	int first = 0;
	__u64 val = 0;
	char key[8];
	sqlite3_stmt *pstmt = NULL;
	time_t now;
	time(&now);
	PREP_SQL(sql1)
	sqlite3_bind_text(pstmt, 1, path, len, NULL);
    	res = sqlite3_step(pstmt);
    	if (res == SQLITE_ROW) { /* this path exists */
    		blob = sqlite3_column_blob(pstmt, 0);
    		memcpy(ptr, blob, sqlite3_column_bytes(pstmt, 0));
    		sqlite3_finalize(pstmt);
      		return 0;
    	}
    	else if (res != SQLITE_DONE) {
      		fprintf(stderr, "SQL exec error: %s\n", sqlite3_errmsg(db));
      		sqlite3_finalize(pstmt);
      		sqlite3_close(db);
      		db = NULL;
      		return -res;
    	}
    	sqlite3_finalize(pstmt);
    	
    	HASH_JEN(path, len, val);
    	memcpy(key, &val, sizeof(val));
    	do {
			PREP_SQL(sql2)
			sqlite3_bind_blob(pstmt, 1, (void*)&key, sizeof(__int64), NULL);
			sqlite3_bind_text(pstmt, 2, path, len, NULL);
   		res = sqlite3_step(pstmt);
   		sqlite3_finalize(pstmt);
   		/*fprintf(stderr, "Key generation res: %d\n", res);
   		fprintf(stderr, "Key generated: %"PRIx64, (unsigned long) (key>>64));
   		fprintf(stderr, "%"PRIx64"\n", (unsigned long) key);
   		fprintf(stderr, "Path is: %s\n", path);
   		fprintf(stderr, "Len of path is: %d Len of half path is: %d\n", len, len/2);*/
   		if (res != SQLITE_CONSTRAINT) break;
   		if (!first++) {
   			fout = fopen("cols","a");
   			fprintf(fout, "\n");
   			fclose(fout);
   			srand(time(NULL));
   		}
   		if (first > 10)
   		{
   			return -res;
   		}
   		new_key = (char*) &key;
   		for(i = 0; i < sizeof(__int64); i++) new_key[i] += (rand() % 256); /* add randomness */
    	} while(1); /* TODO: Should have a set number of retries, this will loop infinitely */
    	if (res != SQLITE_DONE) {
      		fprintf(stderr, "SQL exec error: %s\n", sqlite3_errmsg(db));
      		sqlite3_close(db);
      		db = NULL;
      		return -res;
    	}
    	memcpy(ptr, key, 8);
   	return 0;
}

int get_key(char* path, void* ptr)
{
	char* sql = 	"SELECT key FROM MappingTable \
			 WHERE path = ?1";
	const void* blob;
	int res;
	sqlite3_stmt *pstmt = NULL;
	PREP_SQL(sql)
	sqlite3_bind_text(pstmt, 1, path, strlen(path), NULL);
	res = sqlite3_step(pstmt);
    	if (res == SQLITE_DONE) { /* path doesn't exist in db, not fatal */
    		sqlite3_finalize(pstmt);
    		fprintf(stderr, "SQL ERR is: %d", res);
      		return -res;
    	}
    	else if (res != SQLITE_ROW) {
      		fprintf(stderr, "SQL exec error: %s\n", sqlite3_errmsg(db));
      		sqlite3_close(db);
      		db = NULL;
      		return -res;
    	}
    	blob = sqlite3_column_blob(pstmt, 0);
    	memcpy(ptr, blob, sqlite3_column_bytes(pstmt, 0));
    	sqlite3_finalize(pstmt);
    	return 0;
}

int delete_key(char* path)
{
	char* sql = 	"DELETE FROM MappingTable \
			 WHERE path = ?1";
	int res;
	sqlite3_stmt *pstmt = NULL;
	PREP_SQL(sql)
    	sqlite3_bind_text(pstmt, 1, path, strlen(path), NULL);
    	EXEC_SQL
	return 0;
}

