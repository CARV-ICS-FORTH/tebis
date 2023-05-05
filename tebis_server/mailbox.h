#include <zookeeper/zookeeper.h>

int init_mailbox(zhandle_t *zkandle, char *path);
void receive_from_mailbox(void);
