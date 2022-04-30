#include <stdint.h>
#include <stdio.h>

void spin_loop(volatile int64_t *counter, int64_t threashold);
void wait_for_value(uint32_t *value_addr, uint32_t value);
void field_spin_for_value(volatile uint8_t *value_addr, uint8_t value);
