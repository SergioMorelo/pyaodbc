#ifndef _MODULE_H_
#define _MODULE_H_


#include "aodbc_types.h"


char* get_error_message(const char *fn_name, SQLHANDLE handle, SQLSMALLINT handle_type);
int check_error(PyObject *self, const char *fn_name);
PyMODINIT_FUNC PyInit_pyaodbc(void);

extern int connect_async(Connection *self, const wchar_t *dsn, long long timeout);
extern PyTypeObject Connection_Type;
extern PyTypeObject Cursor_Type;


#endif
