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
#ifndef ZK_UTILS_H
#define ZK_UTILS_H

char *zku_concat_strings(int num, ...);
char *zku_op2String(int rc);
int zku_key_cmp(int key_size_1, const char *key_1, int key_size_2, const char *key_2);
#endif
