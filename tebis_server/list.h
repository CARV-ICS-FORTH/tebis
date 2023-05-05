// Copyright [2020] [FORTH-ICS]
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

#ifndef LIST_H
#define LIST_H

typedef void (*destroy_node_data)(void *data);

struct tebis_klist_node {
	void *data;
	char *key;
	destroy_node_data destroy_data;
	struct tebis_klist_node *prev;
	struct tebis_klist_node *next;
};

struct tebis_klist {
	struct tebis_klist_node *first;
	struct tebis_klist_node *last;
	int mode;
	int size;
};

struct tebis_klist *tebis_klist_init(void);
void *tebis_klist_get_first(struct tebis_klist *list);
void tebis_klist_add_first(struct tebis_klist *list, void *data, const char *data_key, destroy_node_data destroy_data);
void tebis_klist_add_last(struct tebis_klist *list, void *data, const char *data_key, destroy_node_data destroy_data);
void *tebis_klist_remove_first(struct tebis_klist *list);
void *tebis_klist_find_element_with_key(struct tebis_klist *list, char *data_key);
int tebis_klist_remove_element(struct tebis_klist *list, void *data);
int tebis_klist_delete_element(struct tebis_klist *list, void *data);

void tebis_klist_destroy(struct tebis_klist *list);
#endif
