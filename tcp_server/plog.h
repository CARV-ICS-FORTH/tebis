#ifndef PLOG_H
#define PLOG_H

#define PL_ERROR "\e[1m[\e[91mERROR\e[0;1m]\e[0m %s\e[1;33m::\e[0m%s\e[1;33m::\e[0m%d --> \0"
#define PL_WARN "\e[1m[\e[93mWARN\e[0;1m]\e[0m %s\e[1;33m::\e[0m%s\e[1;33m::\e[0m%d --> \0"
#define PL_INFO "\e[1m[\e[92mINFO\e[0;1m]\e[0m %s\e[1;33m::\e[0m%s\e[1;33m::\e[0m%d --> \0"

#define plog(format, ...) plog_internal(format, __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__)

extern int plog_internal(const char *__restrict__ format, const char *__restrict__ file, const char *__restrict__ func,
			 int line, ...);

#endif /** PLOG_H **/
