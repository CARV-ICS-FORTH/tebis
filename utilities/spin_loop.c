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
#include <stdint.h>

uint32_t _read_value(volatile uint32_t *value_addr)
{
	return *value_addr;
}

int64_t get_counter(volatile int64_t *counter)
{
	return *counter;
}
void spin_loop(volatile int64_t *counter, int64_t threashold)
{
	while (get_counter(counter) > threashold) {
	}
}

void wait_for_value(uint32_t *value_addr, uint32_t value)
{
	while (_read_value(value_addr) != value) { /*spin*/
		;
	}
}

void field_spin_for_value(volatile uint8_t *value_addr, uint8_t value)
{
	while (*value_addr != value) { /*spin*/
		;
	}
}
