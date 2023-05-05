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
#include "region_reconfiguration.h"
#include "region_desc.h"

struct region_reconfiguration_s {
	region_desc_t region_desc;
};

static void *reconf_run(void *args)
{
	region_reconfiguration_t region_conf = (region_reconfiguration_t)args;

	return NULL;
}

region_reconfiguration_t reconf_init(region_desc_t region_desc)
{
	region_reconfiguration_t region_conf = calloc(1UL, sizeof(*region_conf));
	region_conf->region_desc = region_desc;
	return region_conf;
}

region_reconfiguration_t reconf_start(region_reconfiguration_t region_conf)
{
}
