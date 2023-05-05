#ifndef BUILD_INDEX_CALLBACKS
#define BUILD_INDEX_CALLBACKS

typedef struct region_desc *region_desc_t;
struct regs_server_desc;

struct build_index_context {
	region_desc_t r_desc;
	struct regs_server_desc *server;
};

void build_index_init_callbacks(struct regs_server_desc *server, struct region_desc *r_desc);
#endif // BUILD_INDEX_CALLBACKS
