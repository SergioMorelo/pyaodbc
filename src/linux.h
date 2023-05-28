#ifndef _LINUX_H_
#define _LINUX_H_


#ifdef __linux__


#include <stdlib.h>
#include <time.h>
#include <pthread.h>
#include <wchar.h>
#include <uchar.h>
#include <sqltypes.h>


typedef struct t_event {
    short state;
    void *obj;
    const wchar_t *str;
    pthread_t thread;
} t_event;


typedef signed long long INT64;
typedef t_event *HANDLE;

#define WAIT_OBJECT_0 0

HANDLE create_t_event();
void close_t_event(HANDLE event);
void u_sleep(unsigned long milliseconds);
short wait_for_single_object(t_event *event, unsigned long milliseconds);
char16_t* wctouc(const wchar_t *wc);
wchar_t* uctowc(char16_t *uc);


#endif


#endif
