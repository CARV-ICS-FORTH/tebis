#ifndef PLOG_H
#define PLOG_H

#define PL_ERROR "\033[1m[\033[91mERROR\033[0;1m]\033[0m %s\033[1;33m::\033[0m%s\033[1;33m::\033[0m%d --> \0"
#define PL_WARN "\033[1m[\033[93mWARN\033[0;1m]\033[0m %s\033[1;33m::\033[0m%s\033[1;33m::\033[0m%d --> \0"
#define PL_INFO "\033[1m[\033[92mINFO\033[0;1m]\033[0m %s\033[1;33m::\033[0m%s\033[1;33m::\033[0m%d --> \0"

#define plog(format, ...) plog_internal(format, __FILE__, __builtin_FUNCTION(), __LINE__, ##__VA_ARGS__)

extern int plog_internal(const char *__restrict__ format, const char *__restrict__ file, const char *__restrict__ func,
			 int line, ...);

#endif /** PLOG_H **/
