#include <stdint.h>
#include <stdio.h>

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
