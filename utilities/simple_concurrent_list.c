// Copyright [2019] [FORTH-ICS]
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
#include <assert.h>
#include <log.h>
#include <stdio.h>
#include <stdlib.h>

#include "simple_concurrent_list.h"

SIMPLE_CONCURRENT_LIST *init_simple_concurrent_list(void)
{
	SIMPLE_CONCURRENT_LIST *list = (SIMPLE_CONCURRENT_LIST *)malloc(sizeof(SIMPLE_CONCURRENT_LIST));
	list->size = 0;
	list->first = NULL;
	list->last = NULL;
	return list;
}

void *get_first_from_simple_concurrent_list(SIMPLE_CONCURRENT_LIST *list)
{
	return list->first;
}

void add_node_in_simple_concurrent_list(SIMPLE_CONCURRENT_LIST *list, SIMPLE_CONCURRENT_LIST_NODE *node)
{
	node->marked_for_deletion = 0;
	node->next = NULL;
	if (list->size == 0) {
		list->last = node;
		list->first = node;
	} else {
		list->last->next = node;
		list->last = node;
	}
	++list->size;
}

void add_last_in_simple_concurrent_list(SIMPLE_CONCURRENT_LIST *list, void *data)
{
	assert(data != NULL);
	SIMPLE_CONCURRENT_LIST_NODE *node = malloc(sizeof(SIMPLE_CONCURRENT_LIST_NODE));
	node->marked_for_deletion = 0;
	node->data = data;
	node->next = NULL;
	if (list->size == 0) {
		list->last = node;
		list->first = node;
	} else {
		list->last->next = node;
		list->last = node;
	}
	++list->size;
}

int mark_element_for_deletion_from_simple_concurrent_list(SIMPLE_CONCURRENT_LIST *list, void *data)
{
	log_debug("\t deletion\n");

	SIMPLE_CONCURRENT_LIST_NODE *node;
	node = list->first;
	while (node != NULL) {
		if (node->data == data) {
			node->marked_for_deletion = 1;
			log_debug("\t marked connection\n");
			return 1;
		}
		node = node->next;
	}
	return 0;
}

void remove_element_from_simple_concurrent_list(SIMPLE_CONCURRENT_LIST *list,
						SIMPLE_CONCURRENT_LIST_NODE *previous_node,
						SIMPLE_CONCURRENT_LIST_NODE *node)
{
	if (previous_node != NULL) {
		previous_node->next = node->next;
		if (node == list->last) {
			list->last = previous_node;
		}
		assert(list->size > 0);
		//log_debug("Nodes now are %d\n",list->size);
		--list->size;
	} else if (previous_node == NULL) {
		if (list->size > 1) {
			assert(list->first == node);
			list->first = node->next;
			assert(list->size > 0);
			assert(list->first != NULL);
			//log_debug("Nodes now are %d\n",list->size);
			--list->size;
		} else {
			list->first = NULL;
			list->last = NULL;
			assert(list->size > 0);
			//log_debug("Nodes now are %d\n",list->size);
			--list->size;
		}
	}
	node->next = NULL;
}

void delete_element_from_simple_concurrent_list(SIMPLE_CONCURRENT_LIST *list,
						SIMPLE_CONCURRENT_LIST_NODE *previous_node,
						SIMPLE_CONCURRENT_LIST_NODE *node)
{
	if (previous_node != NULL) {
		previous_node->next = node->next;
		if (node == list->last) {
			list->last = previous_node;
		}
		assert(list->size > 0);
		--list->size;
	} else if (previous_node == NULL) {
		if (list->size > 1) {
			list->first = node->next;
			--list->size;
		} else {
			list->first = NULL;
			list->last = NULL;
			assert(list->size > 0);
			--list->size;
		}
	}
	node->next = NULL;
	/*data field already freed from spinning thread kernel*/
	free(node);
}
