#ifdef __linux__


#include "linux.h"


HANDLE create_t_event()
{
    t_event *event = (t_event *)malloc(sizeof(t_event));
    if (event == NULL) {
        return NULL;
    }

    event->state = 258;
    event->obj = NULL;
    event->str = NULL;
    event->thread = 0;
    return event;
}


void close_t_event(HANDLE event)
{
    event->state = 258;
    event->obj = NULL;
    event->str = NULL;
    event->thread = 0;

    free(event);
}


void u_sleep(unsigned long milliseconds)
{
    clock_t time_end = clock() + (milliseconds * CLOCKS_PER_SEC / 1000);
    while (clock() < time_end) {}
}


short wait_for_single_object(t_event *event, unsigned long milliseconds)
{
    if (event->state != WAIT_OBJECT_0) {
        u_sleep(milliseconds);
    }
    return event->state;
}


char16_t* wctouc(const wchar_t *wc)
{
    size_t wclen = wcslen(wc) + 1;
    char16_t *uc = (char16_t *)malloc(sizeof(char16_t) * wclen);
    if (uc == NULL) {
        return NULL;
    }

    for (size_t i = 0; i < wclen; i++) {
        uc[i] = wc[i];
    }

    return uc;
}


wchar_t* uctowc(char16_t *uc)
{
    size_t uclen = 0;
    unsigned short *str = uc;
    while (*str) {
        uclen++;
        str++;
    }
    uclen++;  // for '\0'

    wchar_t *wc = (wchar_t *)malloc(sizeof(wchar_t) * uclen);
    if (wc == NULL) {
        return NULL;
    }

    for (size_t i = 0; i < uclen; i++) {
        wc[i] = uc[i];
    }

    return wc;
}


#endif
