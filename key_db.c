#include <sqlite3.h>
#include <linux/types.h>
#include <limits.h>
#include <string.h>
#include <stdio.h>


#define PREP_SQL(QUERY) res = sqlite3_prepare_v2(db, QUERY, -1, &pstmt, NULL); \
    			if (res != SQLITE_OK) { \
      				fprintf(stderr, "SQL prep error: %s\n", sqlite3_errmsg(db)); \
      				sqlite3_close(db); \
      				return -res; \
    			}
    			
#define EXEC_SQL    	res = sqlite3_step(pstmt); \
    			if (res != SQLITE_DONE) { \
      				fprintf(stderr, "SQL exec error: %s\n", sqlite3_errmsg(db)); \
      				sqlite3_finalize(pstmt); \
      				sqlite3_close(db); \
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

static int ret_res_callback(void *res, int argc, char **argv, char **azColName)
{
	__u64 high, low;
	char* ptr = res;
	sscanf(argv[0], "%llu", &low);
	sscanf(argv[1], "%llu", &high);
	/*fprintf(stderr, "Callback generated key: %llu%llu\n", high,low);*/
	memcpy(ptr, &low, sizeof(__u64));
	ptr += sizeof(__u64);
	memcpy(ptr, &high, sizeof(__u64));
	return 0;
}

int create_tables()
{
	char* sql1 = 	"CREATE TABLE MappingTable ( \
    			 keyhi  UNSIGNED INT, \
    			 keylo UNSIGNED INT, \
    			 path TEXT NOT NULL UNIQUE, \
    			 PRIMARY KEY(keyhi, keylo) \
			 );";

	char* sql2 = 	"CREATE INDEX idx_key ON MappingTable (keyhi, keylo); \
			 CREATE INDEX idx_path ON MappingTable (path);";
			
	char* sql3 = 	"CREATE TABLE OpenKeysTable ( \
			 starthi  UNSIGNED INT, \
			 startlo UNSIGNED INT, \
			 endhi UNSIGNED INT, \
			 endlo UNSIGNED INT, \
			 PRIMARY KEY(starthi, startlo) \
			 );";
	char* sql4 = 	"INSERT INTO OpenKeysTable \
			 VALUES(0, 0, ?1, ?1)"; /* allowed range of keys */
			
	int res;
	char* err_msg = NULL;
	sqlite3_stmt *pstmt = NULL;
	res = sqlite3_exec(db, sql1, callback, 0, &err_msg);
	if(res != SQLITE_OK) {
      		fprintf(stderr, "SQL error: %s\n", err_msg);
      		sqlite3_free(err_msg);
      		sqlite3_close(db);
      		return -res;
   	} 
   	res = sqlite3_exec(db, sql2, callback, 0, &err_msg);
   	if(res != SQLITE_OK) {
      		fprintf(stderr, "SQL error: %s\n", err_msg);
      		sqlite3_free(err_msg);
      		sqlite3_close(db);
      		return -res;
   	} 
   	res = sqlite3_exec(db, sql3, callback, 0, &err_msg);
   	if(res != SQLITE_OK) {
      		fprintf(stderr, "SQL error: %s\n", err_msg);
      		sqlite3_free(err_msg);
      		sqlite3_close(db);
      		return -res;
   	} 
   	PREP_SQL(sql4)
    	sqlite3_bind_int64(pstmt, 1, LONG_MAX);
    	EXEC_SQL
   	return 0;
}

int create_db()
{
	int res;
	res = sqlite3_open("keys.db", &db);
	if( res ) {
      		fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
      		sqlite3_close(db);
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
}

int generate_key(char* path, void* key_lo, void* key_hi)
{
	char* sql1 = 	"SELECT keyhi,keylo FROM MappingTable \
			 WHERE path = ?1";
	char* sql2 = 	"SELECT startlo, starthi FROM OpenKeysTable ORDER BY starthi ASC, startlo ASC LIMIT 1";
	int res;
	char* err_msg = NULL, ptr[16] = {0};
	sqlite3_stmt *pstmt = NULL;
	__u64 hi,lo;
	PREP_SQL(sql1)
	sqlite3_bind_text(pstmt, 1, path, strlen(path), NULL);
    	res = sqlite3_step(pstmt);
    	if (res == SQLITE_ROW) { /* this path exists */
    		hi = sqlite3_column_int64(pstmt, 0);
    		lo = sqlite3_column_int64(pstmt, 1);
    		memcpy(key_lo, &lo, sizeof(__u64));
    		memcpy(key_hi, &hi, sizeof(__u64));
    		sqlite3_finalize(pstmt);
    		fprintf(stderr, "Generated key: %llu%llu\n", hi,lo);
      		return 0;
    	}
    	sqlite3_finalize(pstmt);
	res = sqlite3_exec(db, sql2, ret_res_callback, ptr, &err_msg);
	if(res != SQLITE_OK) {
      		fprintf(stderr, "SQL error: %s\n", err_msg);
      		sqlite3_free(err_msg);
      		sqlite3_close(db);
      		return -res;
   	}
   	memcpy(key_lo, ptr, sizeof(__u64));
   	memcpy(key_hi, ptr + sizeof(__u64), sizeof(__u64));
   	return 0;
}

int insert_key(char* path, __u64 high, __u64 low)
{
	char* sql1 = 	"INSERT INTO MappingTable\
			 VALUES(?1, ?2, ?3)";
	char* sql2 = 	"UPDATE OpenKeysTable \
			 SET starthi = ?1, \
			     startlo = ?2 \
			 WHERE (starthi < ?3 OR (starthi = ?3 AND startlo <= ?4)) \
			 	AND (endhi > ?3 OR (endhi = ?3 AND endlo > ?4))";
	char* sql3 = 	"DELETE FROM OpenKeysTable \
			 WHERE starthi = ?1 AND endhi = ?1 AND startlo = ?2 AND endlo = ?2"; /* in case of single key range simply delete it */
	int res;
	__u64 new_hi, new_lo;
	sqlite3_stmt *pstmt = NULL;
	new_lo = low + 1;
	new_hi = high;
	if (!new_lo)
		new_hi++;
	PREP_SQL(sql1)
    	res = sqlite3_bind_int64(pstmt, 1, high);
    	res = sqlite3_bind_int64(pstmt, 2, low);
    	res = sqlite3_bind_text(pstmt, 3, path, strlen(path), NULL);
    	EXEC_SQL
    	PREP_SQL(sql2)
    	sqlite3_bind_int64(pstmt, 1, new_hi);
    	sqlite3_bind_int64(pstmt, 2, new_lo);
    	sqlite3_bind_int64(pstmt, 3, high);
    	sqlite3_bind_int64(pstmt, 4, low);
    	EXEC_SQL
    	if (!sqlite3_changes(db)) { /* 0 changes means if the range exists it has size 1 */
    		PREP_SQL(sql3)
    		sqlite3_bind_int64(pstmt, 1, high);
    		sqlite3_bind_int64(pstmt, 2, low);
    		EXEC_SQL
    	}
    	return 0;
}

int get_key(char* path, void* key_lo, void* key_hi)
{
	char* sql = 	"SELECT keyhi,keylo FROM MappingTable \
			 WHERE path = ?1";
	int res;
	__u64 hi, lo;
	sqlite3_stmt *pstmt = NULL;
	PREP_SQL(sql)
	sqlite3_bind_text(pstmt, 1, path, strlen(path), NULL);
	res = sqlite3_step(pstmt);
    	if (res != SQLITE_ROW) { /* this is not fatal, occurs on invalid path for instance */
    		sqlite3_finalize(pstmt);
    		fprintf(stderr, "SQL ERR is: %d", res);
      		return -res;
    	}
    	hi = sqlite3_column_int64(pstmt, 0);
    	lo = sqlite3_column_int64(pstmt, 1);
    	memcpy(key_lo, &lo, sizeof(__u64));
    	memcpy(key_hi, &hi, sizeof(__u64));
    	sqlite3_finalize(pstmt);
    	return 0;
}

int delete_key(char* path) /* TODO: merge ranges on another thread */
{
	char* sql1 = 	"SELECT keyhi,keylo FROM MappingTable \
			 WHERE path = ?1";
	char* sql2 = 	"DELETE FROM MappingTable \
			 WHERE path = ?1";
	char* sql3 = 	"INSERT INTO OpenKeysTable \
			 VALUES(?1, ?2, ?1, ?2)";
	int res;
	__u64 hi,lo;
	sqlite3_stmt *pstmt = NULL;
	PREP_SQL(sql1)
	sqlite3_bind_text(pstmt, 1, path, strlen(path), NULL);
    	res = sqlite3_step(pstmt);
    	if (res != SQLITE_ROW) { /* this is not fatal, occurs on invalid path for instance */
    		sqlite3_finalize(pstmt);
    		fprintf(stderr, "SQL ERR is: %d", res);
      		return -res;
    	}
    	hi = sqlite3_column_int64(pstmt, 0);
    	lo = sqlite3_column_int64(pstmt, 1);
    	sqlite3_finalize(pstmt);
	PREP_SQL(sql2)
    	sqlite3_bind_text(pstmt, 1, path, strlen(path), NULL);
    	EXEC_SQL
	PREP_SQL(sql3)
    	sqlite3_bind_int64(pstmt, 1, hi);
    	sqlite3_bind_int64(pstmt, 2, lo);
    	EXEC_SQL
	return 0;
}

