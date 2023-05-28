#ifndef _HEADERS_H_
#define _HEADERS_H_


#define PY_SSIZE_T_CLEAN
#include <Python.h>


#ifdef __linux__
#include "linux.h"
#endif

#ifdef _WIN32
#include <Windows.h>
#include <stdlib.h>
#include <wchar.h>
#include <time.h>
#endif

#include <stdio.h>
#include <sql.h>
#include <sqlext.h>
#include <string.h>

#include <locale.h>


#define _DEBUG 0
#define PRINT_DEBUG_MESSAGE(s) {if (_DEBUG) {puts(s);}}

extern int check_error(PyObject *self, const char *fn_name);
#define CHECK_ERROR(s) do { \
   if (check_error((PyObject *)self, s)) { \
       return -1; \
   } \
} while(0)

#define CHECK_EVENT_ERROR(e, s) do { \
    PRINT_DEBUG_MESSAGE(s); \
    if (e == NULL) { \
        PyErr_SetString(PyExc_Exception, s); \
        return -1; \
    } \
} while(0)


#endif
