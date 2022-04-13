#include <stdint.h>
#include <stdio.h>

void spin_loop(volatile int64_t *counter, int64_t threashold);
void wait_for_value(uint8_t *value_addr, uint8_t value);
