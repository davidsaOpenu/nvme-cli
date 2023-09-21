#include <linux/types.h>

int open_db();
void close_db();
int generate_key(char* path, void* key_lo, void* key_hi);
int insert_key(char* path, __u64 high, __u64 low);
int get_key(char* path, void* key_lo, void* key_hi);
int delete_key(char* path);
