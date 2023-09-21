#include <linux/types.h>

int open_db();
void close_db();
int generate_key(char* path, void* ptr);
int get_key(char* path, void* ptr);
int delete_key(char* path);
