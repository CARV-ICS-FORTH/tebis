// Copyright [2023] [FORTH-ICS]
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#ifndef SEND_INDEX_H
#define SEND_INDEX_H
#include "../region_desc.h"
#include "../tebis_rdma/rdma.h"
#include "include/parallax/structures.h"
#include <stdint.h>

/*parameters for function send_index_create_compactions_rdma_buffer*/
struct send_index_create_compactions_rdma_buffer_params {
	region_desc_t r_desc; // the region descriptor of the backup
	connection_rdma *conn; // the rdma connection between the primary and the backup
	uint32_t tree_id; // the tree_id which the wappender and the transaction id will refer to (this is a Parallax thing and it must be 1)
	uint32_t level_id; // the source level of the compaction taking place
	uint32_t number_of_rows; // the height of the compaction index that will be allocated
	uint32_t number_of_columns; // the width of the compaction index that will be allocated
	uint32_t size_of_entry; // the size of each cell in the 2D compaction index that will be allocated
};

/*parameters for function send_index_create_mr_for_segment_replies*/
struct send_index_create_mr_for_segment_replies_params {
	connection_rdma *conn; // the rdma connection between the primary and the backup
	region_desc_t r_desc;
	uint32_t tree_id;
	uint32_t level_id; //the source level of the compaction taking place
};

/*parameters for function 'send_index_flush_index_segment'*/
struct send_index_flush_index_segment_params {
	region_desc_t r_desc; // the region descriptor of the backup
	uint32_t level_id; // the source level of the compaction taking place
	uint32_t height; // the row_id  of the compaction index segment to be flushed
	uint32_t clock; // the col_id of the compaction index segment to be flushed
	uint32_t number_of_columns; // the width of the compaction index that will be allocated
	uint32_t size_of_entry; // the size of each cell in the 2D compaction index
};

/**
 * Send index logic
 * flush the overflown RDMA buffer in appropriate Parallax's log, and update the HashTable that holds the segment mappings
 * @param r_desc: the region desciptor from which the rdma buffer is flushed
 * @param log_type: the type of the buffer to be flushed (L0-recovery, big)
*/
uint64_t send_index_flush_rdma_buffer(region_desc_t r_desc, enum log_category log_type);

void send_index_rewrite_index(struct send_index_rewrite_index_params params);

void add_segment_to_index_HT(struct krm_region_desc *r_desc, uint64_t primary_segment_offt,
			     uint64_t replica_segment_offt, uint32_t level_id);

/**
 * Creates an rdma buffer in which the new compaction index will be stored.
 * For Parallax, the final compaction index is consisted of MAX_HEIGHT lists of segments, each level is connected with the level above.
 * Level 0 are the leafs and Levels > 0 are the index nodes.
 * Tebis creates and wappender Parallax object (for more visit Parallax code base), and allocates a MAX_HEIGHT x COLUMN_NUM array of SEGMENT SIZE segments.
 * Uppon a segment flush, the primary will send the segment to be flushed in the appropriate segment address of the aray
 * @param params: an initialized send_index_create_compactions_rdma_buffer_params struct
 */
void send_index_create_compactions_rdma_buffer(struct send_index_create_compactions_rdma_buffer_params params);

/**
 * Creates and reg writes a buffer that serves as the flush segment reply that will be send to the primary.
 * The buffer is associated with the level_id of the compaction taking place.
 * @param params: an initialized 'send_index_create_mr_for_segment_replies_params' struct
 */
void send_index_create_mr_for_segment_replies(struct send_index_create_mr_for_segment_replies_params params);

/**
 * Frees and rdma deregister the compaction index space that the 'send_index_flush_rdma_buffer' function has allocates
 * @param r_desc: the region descriptr of the backup
 * @param level_id: the source level of the compaction that is taking place
 */
void send_index_close_compactions_rdma_buffer(region_desc_t r_desc, uint32_t level_id);

/**
 * Frees and rdma deregisters the flush segment reply buffers that was allocated by the 'send_index_create_mr_for_segment_replies' function
 * @param r_desc: the region descriptr of the backup
 * @param level_id: the source level of the compaction that is taking place
 */

void send_index_free_index_HT(region_desc_t r_desc, uint32_t level_id);
void send_index_close_mr_for_segment_replies(region_desc_t r_desc, uint32_t level_id);
#endif // SEND_INDEX_H_
