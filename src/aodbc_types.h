#ifndef _AODBC_TYPES_H_
#define _AODBC_TYPES_H_


#include "headers.h"


#define DISCONNECTED 0
#define TO_CONNECT 1
#define CONNECTED 2
#define TO_DISCONNECT 3


typedef struct Connection {
    PyObject_HEAD

    SQLHENV env;
    SQLHDBC handle;
    SQLSMALLINT handle_type;
    SQLRETURN retcode;
    HANDLE event;
    ESTATUS event_status;
    SQLUSMALLINT mca;
    SQLUSMALLINT runned_cursors;
    const wchar_t *dsn;
    long long timeout;
    clock_t start_time;
    double rate;
    unsigned char state:2;
    unsigned char is_exc:1;
} Connection;

#define CLOSED 0
#define TO_OPEN 1
#define OPENED 2
#define TO_EXECUTE 3
#define EXECUTED 4

typedef struct _parameter {
    union value {
        int v_int;
        INT64 v_int64;
        short v_bool;
        double v_float;

        #ifdef _WIN32
        wchar_t *v_str;
        #elif __linux__
        char16_t *v_str;
        #endif

        SQL_TIMESTAMP_STRUCT v_datetime;
        SQL_DATE_STRUCT v_date;
        SQL_TIME_STRUCT v_time;
    } value;
    SQLLEN indicator;
    unsigned char alloc_str:1;
} parameter;

typedef struct parameters_info {
    parameter *parameters;
    Py_ssize_t params_length;
} parameters_info;

typedef struct Cursor {
    PyObject_HEAD

    Connection *conn;
    SQLHSTMT handle;
    SQLSMALLINT handle_type;
    SQLRETURN retcode;
    HANDLE event;
    ESTATUS event_status;
    parameters_info p_info;
    const wchar_t *query;
    long long timeout;
    clock_t start_time;
    unsigned char state:3;
} Cursor;


#endif
