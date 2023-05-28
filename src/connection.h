#ifndef _CONNECTION_H_
#define _CONNECTION_H_


#include "aodbc_types.h"


PyTypeObject ConnectionType;

#ifdef __linux__
void* t_sql_driver_connect_w(void *handle);
void* t_sql_disconnect(void *handle);
extern short wait_for_single_object(t_event *event, unsigned long milliseconds);
extern char16_t* wctouc(const wchar_t *wc);
#endif

int connect_async(Connection *self, const wchar_t *dsn, long long timeout);
SQLUSMALLINT get_max_concurrent_activities(SQLHDBC *handle);
int disconnect_async(Connection *self);

extern int check_error(PyObject *self, const char *fn_name);
extern PyTypeObject Cursor_Type;
extern int allocate_cursor(Cursor *self, Connection *conn);


#endif
