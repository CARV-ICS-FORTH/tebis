#define _GNU_SOURCE
#include <pthread.h>
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <semaphore.h>
#include "djb2.h"
#include "metadata.h"
#include "../utilities/list.h"
#include <log.h>

#define RCO_TASK_QUEUE_SIZE_THREASHOLD 8
extern void on_completion_client(struct rdma_message_context *);

struct rco_db_map_entry {
	struct db_descriptor *db_desc;
	struct krm_region_desc *r_desc;
	struct rco_pool *pool;
	UT_hash_handle hh;
};
static struct rco_db_map_entry *db_map = NULL;
static pthread_mutex_t db_map_lock = PTHREAD_MUTEX_INITIALIZER;

enum rco_task_status {
	RIS_INIT_ENV = 0,
	RIS_SEND_GET_BUFFER_MSG,
	RIS_WAIT_FOR_BUFFER_REP,
	RIS_CHECK_BUFFER_DIRTY,
	RIS_WAIT_FOR_FLUSH_REP,
	RIS_TRANSFER_SEGMENT,
	RIS_SEND_FLUSH_MSG,
	RIS_WAIT_FOR_ALL_PENDING_FLUSHES,
	RIS_COMPLETE
};

struct rco_replica_index_rpc {
	struct sc_msg_pair rdma_buf;
	/*out variables private*/
	uint8_t valid : 1;
	uint8_t reply_pending : 1;
};

struct rco_task {
	// buffers for each replica
	struct ibv_mr remote_mem_buf[RU_MAX_NUM_REPLICAS][MAX_REPLICA_INDEX_BUFFERS];
	sem_t *sem;
	struct segment_header *curr_index_segment;
	struct krm_region_desc *r_desc;
	// my local buf one2many
	struct ibv_mr *local_mem_buf[MAX_REPLICA_INDEX_BUFFERS];
	int local_mem_buf_dirty[MAX_REPLICA_INDEX_BUFFERS];
	int barrier_i;
	int barrier_j;

	struct rco_replica_index_rpc rpc[RU_MAX_NUM_REPLICAS][MAX_REPLICA_INDEX_BUFFERS];
	struct rdma_message_context rpc_ctx[RU_MAX_NUM_REPLICAS][MAX_REPLICA_INDEX_BUFFERS];
	struct connection_rdma *conn[RU_MAX_NUM_REPLICAS];
	enum ru_remote_buffer_status status[RU_MAX_NUM_REPLICAS][MAX_REPLICA_INDEX_BUFFERS];
	uint64_t seg_hash[MAX_REPLICA_INDEX_BUFFERS];
	uint64_t index_offset;
	uint64_t seg_id_to_flush_addr;
	uint32_t seg_id_to_send;
	uint32_t seg_id_to_flush;
	uint32_t msg_id_to_send;
	uint32_t replica_id_cnt;
	int is_seg_last;
	int level_id;
	int tree_id;
	int local_tree_id;
	int num_buffers;
	// group communication variables
	// 1. State of the caller
	// 2. out variable keeps count of how many rpc operations to my replicas have
	// replied
	// uint32_t num_rpcs_complete;
	enum rco_task_status state;
	// enum rco_task_status group_comm_next_state;
	// enum rco_task_status prev_state;
};

static void *rco_compaction_worker(void *args);
static int rco_add_compaction_task(struct rco_pool *pool, struct rco_task *compaction_task);

int rco_init_index_transfer(uint64_t db_id, uint8_t level_id)
{
#if RCO_DISABLE_REMOTE_COMPACTIONS
	return 0;
#endif
	int ret;
	// find krm_region_desc
	// in which pool does this kreon db belongs to?
	struct rco_db_map_entry *db_entry;
	pthread_mutex_lock(&db_map_lock);
	HASH_FIND_PTR(db_map, &db_id, db_entry);
	if (db_entry == NULL) {
		log_fatal("Cannot find pool for db with id %llu", db_id);
		exit(EXIT_FAILURE);
	}
	pthread_mutex_unlock(&db_map_lock);

	// Create an rdma local buffer
	struct krm_region_desc *r_desc = db_entry->r_desc;
	pthread_mutex_lock(&r_desc->region_mgmnt_lock);
	if (r_desc->region->num_of_backup == 0) {
		log_info("Nothing to do for non-replicated region %s", r_desc->region->id);
		ret = 0;
		goto exit;
	}
	struct connection_rdma *r_conn =
		sc_get_compaction_conn(db_entry->pool->rco_server, r_desc->region->backups[0].kreon_ds_hostname);
	char *addr = NULL;
	if (posix_memalign((void **)&addr, ALIGNMENT, SEGMENT_SIZE) != 0) {
		log_fatal("Posix memalign failed");
		perror("Reason: ");
		exit(EXIT_FAILURE);
	}
	r_desc->local_buffer[level_id] = rdma_reg_write(r_conn->rdma_cm_id, addr, SEGMENT_SIZE);
	if (r_desc->local_buffer[level_id] == NULL) {
		log_fatal("Failed to reg memory");
		exit(EXIT_FAILURE);
	}
	ret = 1;
	// Ask from replicas to register a buffer for this procedure
	// SEND_GET_BUFFER_MSG: XXX TODO XXX add support for > 1 replicas
	uint32_t request_size = sizeof(struct msg_replica_index_get_buffer_req);
	uint32_t reply_size = sizeof(struct msg_replica_index_get_buffer_req);
	struct sc_msg_pair rpc_pair;
	do {
		rpc_pair = sc_allocate_rpc_pair(r_conn, request_size, reply_size, REPLICA_INDEX_GET_BUFFER_REQ);
	} while (rpc_pair.stat != ALLOCATION_IS_SUCCESSFULL);

	struct msg_replica_index_get_buffer_req *g_req =
		(struct msg_replica_index_get_buffer_req *)((uint64_t)rpc_pair.request + sizeof(struct msg_header));

	g_req->buffer_size = SEGMENT_SIZE;
	g_req->num_buffers = 1;
	g_req->index_offset = 0; //for explicit IO compactions unused
	g_req->level_id = level_id;
	g_req->region_key_size = r_desc->region->min_key_size;
	if (g_req->region_key_size > RU_REGION_KEY_SIZE) {
		log_fatal("Max region key overflow");
		exit(EXIT_FAILURE);
	}
	memcpy(g_req->region_key, r_desc->region->min_key, g_req->region_key_size);
	rpc_pair.request->session_id = (uint64_t)r_desc->region + level_id;
	rpc_pair.request->request_message_local_addr = rpc_pair.request;
	rpc_pair.reply->receive = TU_RDMA_REGULAR_MSG;
	__send_rdma_message(rpc_pair.conn, rpc_pair.request, NULL);
	// Wait for reply header
	wait_for_value(&rpc_pair.request->receive, TU_RDMA_REGULAR_MSG);
	// Wait for payload arrival
	struct msg_header *reply = rpc_pair.reply;
	uint32_t *tail =
		(uint32_t *)(((uint64_t)reply + sizeof(struct msg_header) + reply->pay_len + reply->padding_and_tail) -
			     TU_TAIL_SIZE);
	wait_for_value(tail, TU_RDMA_REGULAR_MSG);
	/*unroll the reply*/
	struct msg_replica_index_get_buffer_rep *g_rep =
		(struct msg_replica_index_get_buffer_rep *)((uint64_t)reply + sizeof(struct msg_header));

	r_desc->remote_mem_buf[0][level_id] = g_rep->mr[0];
	sc_free_rpc_pair(&rpc_pair);
	memset(&rpc_pair, 0x00, sizeof(struct sc_msg_pair));

exit:
	pthread_mutex_unlock(&r_desc->region_mgmnt_lock);
	return ret;
}

int rco_destroy_local_rdma_buffer(uint64_t db_id, uint8_t level_id)
{
#if RCO_DISABLE_REMOTE_COMPACTIONS
	return 0;
#endif
	int ret;
	// in which pool does this kreon db belongs to?
	struct rco_db_map_entry *db_entry;
	pthread_mutex_lock(&db_map_lock);
	HASH_FIND_PTR(db_map, &db_id, db_entry);
	if (db_entry == NULL) {
		log_fatal("Cannot find pool for db with id %llu", db_id);
		exit(EXIT_FAILURE);
	}
	pthread_mutex_unlock(&db_map_lock);

	struct krm_region_desc *r_desc = db_entry->r_desc;
	pthread_mutex_lock(&r_desc->region_mgmnt_lock);
	if (r_desc->region->num_of_backup == 0) {
		log_info("Nothing to do for non-replicated region %s", r_desc->region->id);
		ret = 0;
		goto exit;
	}
	char *addr = r_desc->local_buffer[level_id]->addr;
	if (rdma_dereg_mr(r_desc->local_buffer[level_id])) {
		log_info("Failed to deregister buffer");
		exit(EXIT_FAILURE);
	}
	free(addr);
	r_desc->local_buffer[level_id] = NULL;
	ret = 1;
exit:
	pthread_mutex_unlock(&r_desc->region_mgmnt_lock);
	return ret;
}

static void rco_wait_flush_reply(struct sc_msg_pair *rpc)
{
	wait_for_value(&rpc->reply->receive, TU_RDMA_REGULAR_MSG);
	// Wait for payload arrival
	struct msg_header *reply = rpc->reply;
	uint32_t *tail =
		(uint32_t *)(((uint64_t)reply + sizeof(struct msg_header) + reply->pay_len + reply->padding_and_tail) -
			     TU_TAIL_SIZE);
	wait_for_value(tail, TU_RDMA_REGULAR_MSG);
	// Check status returned by the replica
	struct msg_replica_index_flush_rep *f_rep =
		(struct msg_replica_index_flush_rep *)((uint64_t)rpc->reply + sizeof(struct msg_header));
	if (f_rep->status != KREON_SUCCESS) {
		log_fatal("Flush index failed for seg_id %d stat is %d", f_rep->seg_id, f_rep->status);
		exit(EXIT_FAILURE);
	}
	sc_free_rpc_pair(rpc);
	memset(rpc, 0x00, sizeof(struct sc_msg_pair));
	return;
}

int rco_send_index_segment_to_replicas(uint64_t db_id, uint64_t dev_offt, struct segment_header *seg, uint32_t size,
				       uint8_t level_id, struct node_header *root)
{
#if RCO_DISABLE_REMOTE_COMPACTIONS
	return 0;
#endif

	int ret = 0;
	// in which pool does this kreon db belongs to?
	struct rco_db_map_entry *db_entry;
	pthread_mutex_lock(&db_map_lock);
	HASH_FIND_PTR(db_map, &db_id, db_entry);
	if (db_entry == NULL) {
		log_fatal("Cannot find pool for db id %llu", db_id);
		exit(EXIT_FAILURE);
	}
	pthread_mutex_unlock(&db_map_lock);

	struct krm_region_desc *r_desc = db_entry->r_desc;

	pthread_mutex_lock(&r_desc->region_mgmnt_lock);

#if 0
	log_info("Sending index segment for region %s", r_desc->region->id);
	char *tmp = (char *)seg; //r_desc->local_buffer[level_id]->addr;
	struct node_header *n = (struct node_header *)((uint64_t)tmp + sizeof(struct segment_header));
	switch (n->type) {
	case leafNode:
	case leafRootNode: {
		log_info("Sending leaf segment to replica for DB:%s", r_desc->db->db_desc->db_name);
		uint32_t decoded_bytes = 4096;
		do {
			assert(n->type == leafNode || n->type == leafRootNode || n->type == paddedSpace);
			n = (struct node_header *)((uint64_t)n + LEAF_NODE_SIZE);
			//log_info("Decoded now are %u", decoded_bytes);
			decoded_bytes += LEAF_NODE_SIZE;
		} while (decoded_bytes < SEGMENT_SIZE);
		assert(decoded_bytes == SEGMENT_SIZE);
		break;
	}
	case internalNode:
	case rootNode:
		log_info("Sending index segment to replica for DB:%s", r_desc->db->db_desc->db_name);
		break;
	case paddedSpace:
		log_info("Sending padded Space to replica for DB:%s", r_desc->db->db_desc->db_name);
		break;
	default:
		log_fatal("This is bullshit");
		assert(0);
		exit(EXIT_FAILURE);
	}
#endif

	if (r_desc->region->num_of_backup == 0) {
		log_info("Nothing to do for non-replicated region %s", r_desc->region->id);
		ret = 0;
		goto exit;
	}
	/*check if the previous index segment has replied*/
	//log_info("rpc in use for level id %d is %d", level_id, r_desc->rpc_in_use[0][level_id]);
	if (r_desc->rpc_in_use[0][level_id]) {
		rco_wait_flush_reply(&r_desc->rpc[0][level_id]);
	}
	r_desc->rpc_in_use[0][level_id] = 0;
	struct connection_rdma *r_conn =
		sc_get_compaction_conn(db_entry->pool->rco_server, r_desc->region->backups[0].kreon_ds_hostname);

	// Sent the segment via RDMA
	if (size > SEGMENT_SIZE) {
		log_fatal("Buffer overflow Sorry");
		exit(EXIT_FAILURE);
	}
	memcpy(r_desc->local_buffer[level_id]->addr, seg, size);
	uint64_t s = djb2_hash((unsigned char *)r_desc->local_buffer[level_id]->addr, SEGMENT_SIZE);
	while (1) {
		//client_rdma_init_message_context(&r_desc->rpc_ctx[0][level_id], NULL);
		//r_desc->rpc_ctx[0][level_id].on_completion_callback = on_completion_client;
		int ret = rdma_post_write(r_conn->rdma_cm_id, NULL /*&r_desc->rpc_ctx[0][level_id]*/,
					  r_desc->local_buffer[level_id]->addr, SEGMENT_SIZE,
					  r_desc->local_buffer[level_id], IBV_SEND_SIGNALED,
					  (uint64_t)r_desc->remote_mem_buf[0][level_id].addr,
					  r_desc->remote_mem_buf[0][level_id].rkey);
		if (ret == 0) {
			break;
		} else if (ret == -1) {
			log_fatal("Failed to send via RDMA reason follows");
			perror("Reason");
			exit(EXIT_FAILURE);
		}
	}

	// Now allocate again we do it for fairness!
	uint32_t request_size = sizeof(struct msg_replica_index_flush_req);
	uint32_t reply_size = sizeof(struct msg_replica_index_flush_rep);

	while (1) {
		r_desc->rpc[0][level_id] =
			sc_allocate_rpc_pair(r_conn, request_size, reply_size, REPLICA_INDEX_FLUSH_REQ);
		if (r_desc->rpc[0][level_id].stat == ALLOCATION_IS_SUCCESSFULL)
			break;
	}

	r_desc->rpc_in_use[0][level_id] = 1;

	struct msg_replica_index_flush_req *f_req =
		(struct msg_replica_index_flush_req *)((uint64_t)r_desc->rpc[0][level_id].request +
						       sizeof(struct msg_header));

	f_req->primary_segment_offt = dev_offt;
	f_req->level_id = level_id;
	f_req->tree_id = 1;
	f_req->seg_id = 0;
	if (root) {
		f_req->is_last = 1;
		f_req->root_r = (uint64_t)root - MAPPED;
		f_req->root_w = 0;
	} else {
		f_req->is_last = 0;
		f_req->root_r = 0;
		f_req->root_w = 0;
	}

	f_req->region_key_size = r_desc->region->min_key_size;
	if (f_req->region_key_size > RU_REGION_KEY_SIZE) {
		log_fatal("Max region key overflow");
		exit(EXIT_FAILURE);
	}
	memcpy(f_req->region_key, r_desc->region->min_key, f_req->region_key_size);

	r_desc->rpc[0][level_id].request->request_message_local_addr = r_desc->rpc[0][level_id].request;
	r_desc->rpc[0][level_id].request->session_id = (uint64_t)r_desc->region + level_id;

	f_req->seg_hash = s;
	//r_desc->rpc[0][level_id].reply->receive = 0;
	__send_rdma_message(r_conn, r_desc->rpc[0][level_id].request, NULL);
	// only for the last wait for the reply with spining
	if (root) {
		//log_info("This was the last index segment waiting for ack");
		rco_wait_flush_reply(&r_desc->rpc[0][level_id]);
		r_desc->rpc_in_use[0][level_id] = 0;
		//log_info("This was the last index segment waiting for ack DONE");
	}
exit:
	pthread_mutex_unlock(&r_desc->region_mgmnt_lock);
	return ret;
}

int rco_flush_last_log_segment(void *handle)
{
#if RCO_DISABLE_REMOTE_COMPACTIONS
	log_info("Ommiting flush last log segment");
	return 1;
#endif
	struct db_handle *hd = (struct db_handle *)handle;
	// in which pool does this kreon db belongs to?
	struct rco_db_map_entry *db_entry;
	pthread_mutex_lock(&db_map_lock);
	HASH_FIND_PTR(db_map, &hd->db_desc, db_entry);
	if (db_entry == NULL) {
		log_fatal("Cannot find pool for db %s", hd->db_desc->db_name);
		exit(EXIT_FAILURE);
	}
	pthread_mutex_unlock(&db_map_lock);

	struct krm_region_desc *r_desc = db_entry->r_desc;
	if (r_desc->region->num_of_backup == 0) {
		log_info("Nothing to do for non-replicated region %s", r_desc->region->id);
		return 1;
	}

	log_info("Setting status of region %s to KRM_HALTED", r_desc->region->id);

	pthread_rwlock_wrlock(&r_desc->kreon_lock);
	if (r_desc->status == KRM_HALTED) {
		log_fatal("Region already halted?");
		exit(EXIT_FAILURE);
	} else {
		r_desc->status = KRM_HALTED;
		pthread_rwlock_unlock(&r_desc->kreon_lock);
		log_info("Successfully set region %s in KRM_HALTED state", r_desc->region->id);
		log_info("Waiting for pending tasks %lld to finish", r_desc->pending_region_tasks);
		spin_loop((int64_t *)&r_desc->pending_region_tasks, 0);
		log_info("Ok all pending tasks finished for region %s", r_desc->pending_region_tasks);
	}

	/*Acquire guard lock and wait writers to finish*/
	int ret = RWLOCK_WRLOCK(&(hd->db_desc->levels[0].guard_of_level.rx_lock));
	if (ret) {
		log_fatal("Failed to acquire guard lock reason");
		switch (ret) {
		case EINVAL:
			log_info("EINVAL");
			break;
		case ENOMEM:
			log_info("ENOMEM");
			break;
		case EBUSY:
			log_info("EBUSY");
			break;
		case EAGAIN:
			log_info("EAGAIN");
			break;
		case EDEADLK:
			log_info("EDEADLK");
			break;
		}
		assert(0);
		exit(EXIT_FAILURE);
	}
	spin_loop(&(hd->db_desc->levels[0].active_writers), 0);
	/*Now check what is the rdma's buffer id which corresponds to the last
* segment*/
	uint64_t lc1 = 0;
	uint64_t lc2 = 0;
	int seg_id_to_flush = -1;
	for (int i = 0; i < RU_REPLICA_NUM_SEGMENTS; i++) {
	retry:
		lc1 = r_desc->m_state->r_buf[0].segment[i].lc1;
		log_info("KV_log_size %llu segment[%d].start = %llu segment[%d].end = %llu", hd->db_desc->KV_log_size,
			 i, r_desc->m_state->r_buf[0].segment[i].start, i, r_desc->m_state->r_buf[0].segment[i].end);
		if (hd->db_desc->KV_log_size > r_desc->m_state->r_buf[0].segment[i].start &&
		    hd->db_desc->KV_log_size <= r_desc->m_state->r_buf[0].segment[i].end) {
			seg_id_to_flush = i;
		}
		lc2 = r_desc->m_state->r_buf[0].segment[i].lc2;
		if (lc1 != lc2)
			goto retry;
		if (seg_id_to_flush != -1)
			break;
	}
	if (seg_id_to_flush == -1) {
		log_fatal("Can't find segment id of the last segment");
		assert(0);
		exit(EXIT_FAILURE);
	}

	/*################# send flush request #########################*/
	struct sc_msg_pair *p =
		(struct sc_msg_pair *)malloc(sizeof(struct sc_msg_pair) * r_desc->region->num_of_backup);
retry_allocate:
	for (uint32_t i = 0; i < r_desc->region->num_of_backup; i++) {
		struct connection_rdma *r_conn = sc_get_compaction_conn(db_entry->pool->rco_server,
									r_desc->region->backups[i].kreon_ds_hostname);
		/*allocate and send command*/
		uint32_t req_size = sizeof(struct msg_flush_cmd_req) + r_desc->region->min_key_size;
		uint32_t rep_size = sizeof(struct msg_flush_cmd_rep);
		p[i] = sc_allocate_rpc_pair(r_conn, req_size, rep_size, FLUSH_COMMAND_REQ);

		if (p[i].stat != ALLOCATION_IS_SUCCESSFULL) {
			// free and retry
			int j = i;
			while (j >= 0) {
				sc_free_rpc_pair(&p[i]);
				--j;
			}
			goto retry_allocate;
		}
	}
	for (uint32_t i = 0; i < r_desc->region->num_of_backup; i++) {
		msg_header *req_header = p[i].request;
		msg_header *rep_header = p[i].reply;
		req_header->request_message_local_addr = req_header;
		req_header->ack_arrived = KR_REP_PENDING;
		/*location where server should put the reply*/
		req_header->reply =
			(char *)((uint64_t)rep_header - (uint64_t)p[i].conn->recv_circular_buf->memory_region);
		req_header->reply_length = sizeof(msg_header) + rep_header->pay_len + rep_header->padding_and_tail;
		/*time to send the message*/
		struct msg_flush_cmd_req *f_req =
			(struct msg_flush_cmd_req *)((uint64_t)req_header + sizeof(struct msg_header));

		/*where primary has stored its segment*/
		f_req->is_partial = 1;
		f_req->log_buffer_id = seg_id_to_flush;
		f_req->master_segment = (uint64_t)hd->db_desc->KV_log_last_segment - MAPPED;
		f_req->segment_id = hd->db_desc->KV_log_last_segment->segment_id;
		f_req->end_of_log = hd->db_desc->KV_log_size;
		f_req->log_padding = SEGMENT_SIZE - (hd->db_desc->KV_log_size % SEGMENT_SIZE);
		f_req->region_key_size = r_desc->region->min_key_size;
		strcpy(f_req->region_key, r_desc->region->min_key);
		__send_rdma_message(p[i].conn, req_header, NULL);

		log_info("Compaction daemon Sent flush command for last segment waiting "
			 "for replies seg id is %llu",
			 f_req->segment_id);
	}

	for (uint32_t i = 0; i < r_desc->region->num_of_backup; i++) {
		/*check if header is there*/
		msg_header *reply = p[i].reply;
		wait_for_value(&reply->receive, TU_RDMA_REGULAR_MSG);
		/*check if payload is there*/
		uint32_t *tail = (uint32_t *)(((uint64_t)reply + sizeof(struct msg_header) + reply->pay_len +
					       reply->padding_and_tail) -
					      TU_TAIL_SIZE);
		wait_for_value(tail, TU_RDMA_REGULAR_MSG);
	}
	log_info("Send and acked flush last value log segment ready to compact");
	/*##############################################################*/

	/*Release guard lock*/
	if (RWLOCK_UNLOCK(&hd->db_desc->levels[0].guard_of_level.rx_lock)) {
		log_fatal("Failed to release guard lock");
		exit(EXIT_FAILURE);
	}

	pthread_rwlock_wrlock(&r_desc->kreon_lock);
	r_desc->status = KRM_OPEN;
	pthread_rwlock_unlock(&r_desc->kreon_lock);
	for (uint32_t i = 0; i < r_desc->region->num_of_backup; i++) {
		sc_free_rpc_pair(&p[i]);
	}
	free(p);
	return 1;
}

int rco_send_index_to_group(struct bt_compaction_callback_args *c)
{
#if RCO_DISABLE_REMOTE_COMPACTIONS
	log_info("ommiting");
	return 1;
#endif
	// in which pool does this kreon db belongs to?
	struct rco_db_map_entry *db_entry;

	pthread_mutex_lock(&db_map_lock);
	HASH_FIND_PTR(db_map, &c->db_desc, db_entry);
	if (db_entry == NULL) {
		log_fatal("Cannot find pool for db %s", c->db_desc->db_name);
		exit(EXIT_FAILURE);
	}
	pthread_mutex_unlock(&db_map_lock);

	if (db_entry->r_desc->region->num_of_backup == 0) {
		log_warn("Nothing to do for non replicated region %s", db_entry->r_desc->region->id);
		return 1;
	}
	struct rco_task *t = (struct rco_task *)malloc(sizeof(struct rco_task));
	memset(t, 0x00, sizeof(struct rco_task));
	t->r_desc = db_entry->r_desc;
	assert(t->r_desc != NULL);
	t->curr_index_segment = t->r_desc->db->db_desc->levels[c->dst_level].first_segment[c->dst_local_tree];
	t->index_offset = t->r_desc->db->db_desc->levels[c->dst_level].offset[c->dst_local_tree];
	log_info("Segments of index[%d][%d]", c->dst_level, c->dst_local_tree);
	// struct segment_header *S = t->curr_index_segment;
	// int id = 0;
	// while(1){
	//	log_info("Seg no %d",id);
	//	id++;
	//	if(S->next_segment == NULL)
	//		break;
	//	S = MAPPED+S->next_segment;
	//}
	assert(t->curr_index_segment != NULL);
	t->seg_id_to_send = 0;
	t->seg_id_to_flush = 0;
	t->level_id = c->dst_level;
	t->tree_id = c->dst_remote_tree;
	t->local_tree_id = c->dst_local_tree;
	t->state = RIS_INIT_ENV;

	t->sem = &c->sem;
	if (sem_init(t->sem, 0, 0)) {
		log_fatal("Failed to init sem");
		exit(EXIT_FAILURE);
	}
	/*fill connections*/
	for (uint32_t i = 0; i < t->r_desc->region->num_of_backup; i++)
		t->conn[i] = sc_get_compaction_conn(db_entry->pool->rco_server,
						    t->r_desc->region->backups[i].kreon_ds_hostname);
	rco_add_compaction_task(db_entry->pool, t);

	sem_wait(t->sem);
	log_info("Done sending index for region %s", t->r_desc->region->id);
	free(t);
	return 1;
}

static void rco_send_index_to_replicas(struct rco_task *task)
{
	while (1) {
		switch (task->state) {
		case RIS_INIT_ENV: {
			task->replica_id_cnt = 0;
			task->seg_id_to_send = 0;
			task->seg_id_to_flush = 0;
			for (int i = 0; i < MAX_REPLICA_INDEX_BUFFERS; i++) {
				void *mem = malloc(SEGMENT_SIZE);
				task->local_mem_buf[i] = rdma_reg_write(task->conn[0]->rdma_cm_id, mem, SEGMENT_SIZE);
				if (task->local_mem_buf[i] == NULL) {
					log_fatal("Failed to reg memory");
					exit(EXIT_FAILURE);
				}
				task->local_mem_buf_dirty[i] = 0;
			}
			task->state = RIS_SEND_GET_BUFFER_MSG;
			break;
		}

		case RIS_SEND_GET_BUFFER_MSG: {
			if (task->replica_id_cnt >= task->r_desc->region->num_of_backup) {
				log_info("Done with buffers let's go to transfer");
				task->replica_id_cnt = 0;
				task->state = RIS_CHECK_BUFFER_DIRTY;
				break;
			}
			uint32_t request_size = sizeof(struct msg_replica_index_get_buffer_req);
			uint32_t reply_size = sizeof(struct msg_replica_index_get_buffer_req);
			task->rpc[task->replica_id_cnt][0].rdma_buf =
				sc_allocate_rpc_pair(task->conn[task->replica_id_cnt], request_size, reply_size,
						     REPLICA_INDEX_GET_BUFFER_REQ);
			if (task->rpc[task->replica_id_cnt][0].rdma_buf.stat != ALLOCATION_IS_SUCCESSFULL)
				return;
			struct msg_replica_index_get_buffer_req *g_req =
				(struct msg_replica_index_get_buffer_req *)((uint64_t)task->rpc[task->replica_id_cnt][0]
										    .rdma_buf.request +
									    sizeof(struct msg_header));

			g_req->buffer_size = SEGMENT_SIZE;
			g_req->num_buffers = MAX_REPLICA_INDEX_BUFFERS;
			g_req->index_offset = task->index_offset;
			g_req->level_id = task->level_id;
			g_req->region_key_size = task->r_desc->region->min_key_size;
			if (g_req->region_key_size > RU_REGION_KEY_SIZE) {
				log_fatal("Max region key overflow");
				exit(EXIT_FAILURE);
			}
			memcpy(g_req->region_key, task->r_desc->region->min_key, g_req->region_key_size);
			task->rpc[task->replica_id_cnt][0].rdma_buf.request->session_id =
				(uint64_t)task->r_desc->region + task->level_id;
			task->rpc[task->replica_id_cnt][0].rdma_buf.request->request_message_local_addr =
				task->rpc[task->replica_id_cnt][0].rdma_buf.request;
			__send_rdma_message(task->rpc[task->replica_id_cnt][0].rdma_buf.conn,
					    task->rpc[task->replica_id_cnt][0].rdma_buf.request, NULL);

			task->state = RIS_WAIT_FOR_BUFFER_REP;
			log_info("Send request for replica %d", task->replica_id_cnt);
			break;
		}
		case RIS_WAIT_FOR_BUFFER_REP: {
			if (task->rpc[task->replica_id_cnt][0].rdma_buf.request->receive != TU_RDMA_REGULAR_MSG)
				return;
			struct msg_header *reply = task->rpc[task->replica_id_cnt][0].rdma_buf.reply;
			uint32_t *tail = (uint32_t *)(((uint64_t)reply + sizeof(struct msg_header) + reply->pay_len +
						       reply->padding_and_tail) -
						      TU_TAIL_SIZE);

			if (*tail != TU_RDMA_REGULAR_MSG)
				return;
			/*unroll the reply*/
			log_info("Got buffers from replica id %u from my group for db: %s of "
				 "tree[%d][%d]",
				 task->replica_id_cnt, task->r_desc->db->db_desc->db_name, task->level_id,
				 task->tree_id);
			struct msg_replica_index_get_buffer_rep *g_rep =
				(struct msg_replica_index_get_buffer_rep *)((uint64_t)reply +
									    sizeof(struct msg_header));

			for (int j = 0; j < MAX_REPLICA_INDEX_BUFFERS; j++) {
				task->remote_mem_buf[task->replica_id_cnt][j] = g_rep->mr[j];
			}
			sc_free_rpc_pair(&task->rpc[task->replica_id_cnt][0].rdma_buf);
			memset(&task->rpc[task->replica_id_cnt][0].rdma_buf, 0x00, sizeof(struct sc_msg_pair));
			++task->replica_id_cnt;
			task->state = RIS_SEND_GET_BUFFER_MSG;
			break;
		}
		case RIS_CHECK_BUFFER_DIRTY: {
			int seg_id = task->seg_id_to_send % MAX_REPLICA_INDEX_BUFFERS;
			if (task->local_mem_buf_dirty[seg_id]) {
				// log_info("Buffer dirty wait for flush acks");
				task->state = RIS_WAIT_FOR_FLUSH_REP;
				break;
			}
			// log_info("buffer ok send rdma write with the segment");
			for (uint32_t i = 0; i < task->r_desc->region->num_of_backup; i++) {
				task->rpc[i][seg_id].valid = 0;
				task->rpc[i][seg_id].reply_pending = 1;
			}
			task->state = RIS_TRANSFER_SEGMENT;
			break;
		}

		case RIS_WAIT_FOR_FLUSH_REP: {
			int seg_id = task->seg_id_to_send % MAX_REPLICA_INDEX_BUFFERS;
			for (uint32_t i = 0; i < task->r_desc->region->num_of_backup; i++) {
				if (task->rpc[i][seg_id].valid && task->rpc[i][seg_id].reply_pending) {
					if (task->rpc[i][seg_id].rdma_buf.request->receive != TU_RDMA_REGULAR_MSG)
						return;
					struct msg_header *reply = task->rpc[i][seg_id].rdma_buf.reply;
					uint32_t *tail = (uint32_t *)(((uint64_t)reply + sizeof(struct msg_header) +
								       reply->pay_len + reply->padding_and_tail) -
								      TU_TAIL_SIZE);

					if (*tail != TU_RDMA_REGULAR_MSG)
						return;
					/*unroll the reply*/
					// log_info("Got flush rep from replica id %u  for seg_id: %d from my
					// group "
					//	 "for db: %s of tree[%d][%d]",
					//	 i, task->seg_id_to_send, task->r_desc->db->db_desc->db_name,
					//	 task->level_id, task->tree_id);

					struct msg_replica_index_flush_rep *f_rep =
						(struct msg_replica_index_flush_rep *)((uint64_t)task->rpc[i][seg_id]
											       .rdma_buf.reply +
										       sizeof(struct msg_header));
					if (f_rep->status != KREON_SUCCESS) {
						log_fatal("Flush index failed for seg_id %d stat is %d", f_rep->seg_id,
							  f_rep->status);
						exit(EXIT_FAILURE);
					}
					sc_free_rpc_pair(&task->rpc[i][seg_id].rdma_buf);
					memset(&task->rpc[i][seg_id].rdma_buf, 0x00, sizeof(struct sc_msg_pair));
					task->rpc[i][seg_id].reply_pending = 0;
				}
			}
			task->local_mem_buf_dirty[seg_id] = 0;
			task->state = RIS_CHECK_BUFFER_DIRTY;
			break;
		}
		case RIS_TRANSFER_SEGMENT: {
			// read index from kreon and send it
			assert(task->curr_index_segment != NULL);
			int seg_id = task->seg_id_to_send % MAX_REPLICA_INDEX_BUFFERS;
			/*copy index segment to the local rdma buffer*/
			memcpy(task->local_mem_buf[seg_id]->addr, task->curr_index_segment, SEGMENT_SIZE);
			task->seg_hash[seg_id] =
				djb2_hash((unsigned char *)task->local_mem_buf[seg_id]->addr, SEGMENT_SIZE);
			log_info("Hash for seg[%u] = %llu seg id (to send) %lu", seg_id, task->seg_hash[seg_id],
				 task->seg_id_to_send);
			task->local_mem_buf_dirty[seg_id] = 1;
			// custom dbg to be removed
			uint32_t *type = (uint32_t *)((char *)task->curr_index_segment + sizeof(segment_header));
			switch (*type) {
			case leafNode:
			case internalNode:
			case rootNode:
			case leafRootNode:
			case keyBlockHeader:
			case paddedSpace:
				break;
			default:
				log_fatal("Corrupted index segment");
				assert(0);
			}
			/*send it to the group via rdma writes*/
			for (uint32_t i = 0; i < task->r_desc->region->num_of_backup; i++) {
				while (1) {
					client_rdma_init_message_context(&task->rpc_ctx[i][seg_id], NULL);
					task->rpc_ctx[i][seg_id].on_completion_callback = on_completion_client;
					int ret = rdma_post_write(task->conn[i]->rdma_cm_id, &task->rpc_ctx[i][seg_id],
								  task->local_mem_buf[seg_id]->addr, SEGMENT_SIZE,
								  task->local_mem_buf[seg_id], IBV_SEND_SIGNALED,
								  (uint64_t)task->remote_mem_buf[i][seg_id].addr,
								  task->remote_mem_buf[i][seg_id].rkey);
					if (ret == 0) {
						// log_info("Done sending index segment");
						break;
					} else if (ret == -1) {
						log_fatal("failured reason");
						perror("Reason");
						exit(EXIT_FAILURE);
					}
				}
			}

			task->seg_id_to_flush = task->seg_id_to_send;
			task->seg_id_to_flush_addr = (uint64_t)task->curr_index_segment - MAPPED;

			if (task->curr_index_segment->next_segment == NULL) {
				log_info("Done sending seg %d this was the last one with rdma write "
					 "now send a flush command",
					 task->seg_id_to_send);
				// task->curr_index_segment = NULL;
				task->is_seg_last = 1;
			} else {
				task->curr_index_segment =
					(struct segment_header *)(MAPPED + task->curr_index_segment->next_segment);
				// log_info("Done sending seg %lu more to come with rdma write "
				//	 "now send a flush command",
				//		 task->seg_id_to_send);
				task->is_seg_last = 0;
			}
			++task->seg_id_to_send;
			task->state = RIS_SEND_FLUSH_MSG;
			break;
		}
		case RIS_WAIT_FOR_ALL_PENDING_FLUSHES: {
			for (int j = task->barrier_j; j < MAX_REPLICA_INDEX_BUFFERS; j++) {
				for (uint32_t i = task->barrier_i; i < task->r_desc->region->num_of_backup; i++) {
					if (task->rpc[i][j].valid && task->rpc[i][j].reply_pending) {
						assert(task->local_mem_buf_dirty[j] == 1);
						/*check if reply has arrived*/
						if (task->rpc[i][j].rdma_buf.request->receive != TU_RDMA_REGULAR_MSG) {
							task->barrier_i = i;
							task->barrier_j = j;
							return;
						}
						struct msg_header *reply = task->rpc[i][j].rdma_buf.reply;
						uint32_t *tail =
							(uint32_t *)(((uint64_t)reply + sizeof(struct msg_header) +
								      reply->pay_len + reply->padding_and_tail) -
								     TU_TAIL_SIZE);

						if (*tail != TU_RDMA_REGULAR_MSG) {
							task->barrier_i = i;
							task->barrier_j = j;
							return;
						}

						struct msg_replica_index_flush_rep *f_rep =
							(struct msg_replica_index_flush_rep
								 *)((uint64_t)task->rpc[task->replica_id_cnt][j]
									    .rdma_buf.reply +
								    sizeof(struct msg_header));
						if (f_rep->status != KREON_SUCCESS) {
							log_fatal("Flush index failed for seg_id %d stat is %d",
								  f_rep->seg_id, f_rep->status);
							exit(EXIT_FAILURE);
						}
						sc_free_rpc_pair(&task->rpc[i][j].rdma_buf);
						memset(&task->rpc[i][j].rdma_buf, 0x00, sizeof(struct sc_msg_pair));
						task->rpc[i][j].reply_pending = 0;

						// log_info("SUCCESS! for Flush index failed for seg_id %d stat is
						// %d",
						//	 f_rep->seg_id, f_rep->status);
					}
				}
				task->local_mem_buf_dirty[task->barrier_j] = 0;
			}
			if (task->is_seg_last)
				task->state = RIS_COMPLETE;
			else
				task->state = RIS_SEND_FLUSH_MSG;
			break;
		}
		case RIS_COMPLETE: {
			for (int i = 0; i < MAX_REPLICA_INDEX_BUFFERS; i++) {
				free(task->local_mem_buf[i]->addr);
				if (rdma_dereg_mr(task->local_mem_buf[i])) {
					log_info("Failed to deregister buffer");
					exit(EXIT_FAILURE);
				}
			}
			return;
		}
		case RIS_SEND_FLUSH_MSG: {
			uint32_t request_size = sizeof(struct msg_replica_index_flush_req);
			uint32_t reply_size = sizeof(struct msg_replica_index_flush_rep);
			int seg_id = task->seg_id_to_flush % MAX_REPLICA_INDEX_BUFFERS;
			for (uint32_t i = 0; i < task->r_desc->region->num_of_backup; i++) {
				if (task->rpc[i][seg_id].valid)
					continue;

				task->rpc[i][seg_id].rdma_buf = sc_allocate_rpc_pair(
					task->conn[i], request_size, reply_size, REPLICA_INDEX_FLUSH_REQ);
				if (task->rpc[task->replica_id_cnt][seg_id].rdma_buf.stat !=
				    ALLOCATION_IS_SUCCESSFULL) {
					task->state = RIS_WAIT_FOR_ALL_PENDING_FLUSHES;
					break;
				}
				struct msg_replica_index_flush_req *f_req =
					(struct msg_replica_index_flush_req *)((uint64_t)task->rpc[i][seg_id]
										       .rdma_buf.request +
									       sizeof(struct msg_header));

				assert(task->curr_index_segment != NULL);
				f_req->primary_segment_offt = task->seg_id_to_flush_addr;
				f_req->level_id = task->level_id;
				f_req->tree_id = task->tree_id;
				f_req->seg_id = task->seg_id_to_flush;
				f_req->seg_hash = task->seg_hash[f_req->seg_id % MAX_REPLICA_INDEX_BUFFERS];
				log_info("Attached hash seg[%u] = %llu", f_req->seg_id % MAX_REPLICA_INDEX_BUFFERS,
					 f_req->seg_hash);
				f_req->is_last = task->is_seg_last;
				if (task->is_seg_last) {
					if (task->r_desc->db->db_desc->levels[task->level_id]
						    .root_w[task->local_tree_id] == NULL)
						f_req->root_w = 0;
					else
						f_req->root_w =
							(uint64_t)task->r_desc->db->db_desc->levels[task->level_id]
								.root_w[task->local_tree_id] -
							MAPPED;
					if (task->r_desc->db->db_desc->levels[task->level_id]
						    .root_r[task->local_tree_id] == NULL)
						f_req->root_r = 0;
					else
						f_req->root_r =
							(uint64_t)task->r_desc->db->db_desc->levels[task->level_id]
								.root_r[task->local_tree_id] -
							MAPPED;
				}

				f_req->region_key_size = task->r_desc->region->min_key_size;
				if (f_req->region_key_size > RU_REGION_KEY_SIZE) {
					log_fatal("Max region key overflow");
					exit(EXIT_FAILURE);
				}
				memcpy(f_req->region_key, task->r_desc->region->min_key, f_req->region_key_size);

				task->rpc[i][seg_id].rdma_buf.request->request_message_local_addr =
					task->rpc[i][seg_id].rdma_buf.request;
				task->rpc[i][seg_id].rdma_buf.request->session_id =
					(uint64_t)task->r_desc->region + task->level_id;
				__send_rdma_message(task->rpc[i][seg_id].rdma_buf.conn,
						    task->rpc[i][seg_id].rdma_buf.request, NULL);
				task->rpc[i][seg_id].valid = 1;
			}
			if (task->is_seg_last) {
				task->state = RIS_WAIT_FOR_ALL_PENDING_FLUSHES;
				break;
			}
			task->state = RIS_CHECK_BUFFER_DIRTY;
			break;
		}

		default: {
			log_fatal("Invalid state");
			exit(EXIT_FAILURE);
		} break;
		}
	}
}

void rco_add_db_to_pool(struct rco_pool *pool, struct krm_region_desc *r_desc)
{
	pthread_mutex_lock(&db_map_lock);
	struct rco_db_map_entry *e = (struct rco_db_map_entry *)malloc(sizeof(struct rco_db_map_entry));
	e->db_desc = r_desc->db->db_desc;
	e->r_desc = r_desc;
	e->pool = pool;
	HASH_ADD_PTR(db_map, db_desc, e);
	pthread_mutex_unlock(&db_map_lock);
}

struct rco_pool *rco_init_pool(struct krm_server_desc *server, int pool_size)
{
	struct rco_pool *pool = NULL;
	pool = (struct rco_pool *)malloc(sizeof(struct rco_pool) + (pool_size * sizeof(struct rco_task_queue)));
	pool->num_workers = pool_size;
	pool->curr_worker_id = 0;
	pool->rco_server = server;
	if (pthread_mutex_init(&pool->pool_lock, NULL)) {
		log_fatal("Failed to initiliaze compaction pool lock");
		exit(EXIT_FAILURE);
	}

	for (int i = 0; i < pool_size; i++) {
		if (pthread_mutex_init(&pool->worker_queue[i].queue_lock, NULL)) {
			log_fatal("Failed to initialize queue lock for compaction threads pool");
			exit(EXIT_FAILURE);
		}
		if (pthread_cond_init(&pool->worker_queue[i].queue_monitor, NULL)) {
			log_fatal("Failed to initialize queue monitor");
			exit(EXIT_FAILURE);
		}
		pool->worker_queue[i].task_queue = klist_init();
		pool->worker_queue[i].my_id = i;
		if (pthread_create(&pool->worker_queue[i].cnxt, NULL, rco_compaction_worker, &pool->worker_queue[i]) !=
		    0) {
			log_fatal("Failed to start remote compaction worker");
			exit(EXIT_FAILURE);
		}
	}
	return pool;
}

static int rco_add_compaction_task(struct rco_pool *pool, struct rco_task *compaction_task)
{
	int chosen_id;
	pthread_mutex_lock(&pool->pool_lock);
#if 0
	if (pool->worker_queue[pool->curr_worker_id].task_queue->size <= RCO_TASK_QUEUE_SIZE_THREASHOLD)
		chosen_id = pool->curr_worker_id;
	else {
		int min_id_working = -1;
		int min_tasks = 10000000;
		int min_id = -1;
		/*find someone*/
		for (int i = 0; i < pool->num_workers; i++) {
			if (!pool->worker_queue[pool->curr_worker_id].sleeping &&
			    pool->worker_queue[pool->curr_worker_id].task_queue->size <
				    RCO_TASK_QUEUE_SIZE_THREASHOLD) {
				min_id_working = i;
			}
			if (min_tasks > pool->worker_queue[pool->curr_worker_id].task_queue->size) {
				min_tasks = pool->worker_queue[pool->curr_worker_id].task_queue->size;
				min_id = i;
			}
		}
		if (min_id_working != -1)
			chosen_id = min_id_working;
		else
			chosen_id = min_id;
	}
#endif
	uint64_t session_id = (uint64_t)compaction_task->r_desc->region + compaction_task->level_id;
	uint64_t hash = djb2_hash((unsigned char *)&session_id, sizeof(uint64_t));
	chosen_id = hash % pool->num_workers;

	pool->curr_worker_id = chosen_id;
	// log_info("Assigning to worker %d", pool->curr_worker_id);
	pthread_mutex_unlock(&pool->pool_lock);

	pthread_mutex_lock(&pool->worker_queue[chosen_id].queue_lock);
	klist_add_last(pool->worker_queue[chosen_id].task_queue, compaction_task, NULL, NULL);
	if (pool->worker_queue[chosen_id].sleeping) {
		// log_info("guy is sleeping wake him up");
		if (pthread_cond_broadcast(&pool->worker_queue[chosen_id].queue_monitor) != 0) {
			log_fatal("Failed to wake up compaction worker");
			exit(EXIT_FAILURE);
		}
	}
	pthread_mutex_unlock(&pool->worker_queue[chosen_id].queue_lock);
	return 1;
}

#if RCO_BUILD_INDEX_AT_REPLICA
void rco_build_index(struct rco_build_index_task *task)
{
	struct rco_key {
		uint32_t size;
		char key[];
	};
	struct rco_value {
		uint32_t size;
		char value[];
	};

	// parse log entries
#if RCO_EXPLICIT_IO
	char *kv = NULL;
#else
	char *kv = malloc(PREFIX_SIZE + sizeof(char *));
#endif
	struct rco_key *key = NULL;
	struct rco_value *value = NULL;
	struct segment_header *curr_segment = task->segment;
	struct db_descriptor *db_desc = task->r_desc->db->db_desc;
	uint64_t log_offt = task->log_start;
	key = (struct rco_key *)((uint64_t)curr_segment + sizeof(struct segment_header));

	uint32_t remaining = SEGMENT_SIZE - sizeof(struct segment_header);
	while (1) {
		db_desc->dirty = 0x01;
		struct bt_insert_req ins_req;
		ins_req.metadata.handle = task->r_desc->db;
		ins_req.metadata.level_id = 0;
		ins_req.metadata.tree_id = 0; // will be filled properly by the engine
		ins_req.metadata.special_split = 0;
#if RCO_EXPLICIT_IO
		ins_req.metadata.key_format = KV_FORMAT;
		ins_req.metadata.append_to_log = 1;
		kv = (char *)key;
#else
		ins_req.metadata.key_format = KV_PREFIX;
		ins_req.metadata.append_to_log = 0;
		int bytes_to_copy = PREFIX_SIZE;
		if (key->size < PREFIX_SIZE) {
			memset(kv, 0x00, PREFIX_SIZE);
			bytes_to_copy = key->size;
		}
		memcpy(kv, key->key, bytes_to_copy);
		*(uint64_t *)(kv + PREFIX_SIZE) = (uint64_t)key;
#endif
		ins_req.key_value_buf = kv;
		int active_tree = task->r_desc->db->db_desc->levels[0].active_tree;
		if (db_desc->levels[0].level_size[active_tree] > db_desc->levels[0].max_level_size) {
			pthread_mutex_lock(&db_desc->client_barrier_lock);
			active_tree = db_desc->levels[0].active_tree;

			if (db_desc->levels[0].level_size[active_tree] > db_desc->levels[0].max_level_size) {
				sem_post(&db_desc->compaction_daemon_interrupts);
				if (pthread_cond_wait(&db_desc->client_barrier, &db_desc->client_barrier_lock) != 0) {
					log_fatal("failed to throttle");
					exit(EXIT_FAILURE);
				}
			}
			pthread_mutex_unlock(&db_desc->client_barrier_lock);
		}
		//log_info("Adding index entry for key %u:%s offset %llu log end %llu",
		//key->size, key->key, log_offt, task->log_end);
		remaining -= (sizeof(struct rco_key) + key->size);
		_insert_key_value(&ins_req);
		value = (struct rco_value *)((uint64_t)key + sizeof(struct rco_key) + key->size);
		assert(value->size < 1200);
		remaining -= (sizeof(struct rco_value) + value->size);
		log_offt += (sizeof(struct rco_key) + key->size + sizeof(struct rco_value) + value->size);
		key = (struct rco_key *)((uint64_t)key + sizeof(struct rco_key) + key->size + sizeof(struct rco_value) +
					 value->size);
		if (task->log_end - log_offt < sizeof(uint32_t) || log_offt >= task->log_end) {
			//log_info("log_offt exceeded ok!");
			break;
		}
		if (remaining >= sizeof(struct rco_key) && key->size == 0) {
			break;
		} else
			break;
	}
	//log_info("Done parsing segment");
#if !RCO_EXPLICIT_IO
	free(kv);
#endif
	return;
}
#endif

static void *rco_compaction_worker(void *args)
{
	struct rco_task_queue *my_queue = (struct rco_task_queue *)args;
	pthread_setname_np(pthread_self(), "rco_worker");

	while (1) {
		struct klist_node *node = NULL;
		while (node == NULL) {
			/*get something from the queue otherwise sleep*/
			pthread_mutex_lock(&my_queue->queue_lock);
			node = klist_remove_first(my_queue->task_queue);
			if (node == NULL) {
				my_queue->sleeping = 1;
				if (pthread_cond_wait(&my_queue->queue_monitor, &my_queue->queue_lock) != 0) {
					log_fatal("failed to sleep");
					exit(EXIT_FAILURE);
				}
				my_queue->sleeping = 0;
			}
			pthread_mutex_unlock(&my_queue->queue_lock);
		}

		struct rco_task *t = (struct rco_task *)node->data;
		rco_send_index_to_replicas(t);
		if (t->state == RIS_COMPLETE) {
			free(node);
			sem_post(t->sem);
		} else {
			pthread_mutex_lock(&my_queue->queue_lock);
			klist_add_last(my_queue->task_queue, node, NULL, NULL);
			pthread_mutex_unlock(&my_queue->queue_lock);
		}
	}
}
