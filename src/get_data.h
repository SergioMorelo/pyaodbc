#ifndef _GET_DATA_H_
#define _GET_DATA_H_


#include "aodbc_types.h"
#include <math.h>
#include <datetime.h>


#ifdef __linux__
extern wchar_t* uctowc(char16_t *uc);
#endif

PyObject* get_integer_smallint(Cursor *self, SQLUSMALLINT ColumnNumber);
PyObject* get_bigint(Cursor *self, SQLUSMALLINT ColumnNumber);
PyObject* get_numeric_decimal(Cursor *self, SQLUSMALLINT ColumnNumber);
PyObject* get_bit(Cursor *self, SQLUSMALLINT ColumnNumber);
PyObject* get_tinyint(Cursor *self, SQLUSMALLINT ColumnNumber);
PyObject* get_float(Cursor *self, SQLUSMALLINT ColumnNumber);
PyObject* get_float_real_double(Cursor *self, SQLUSMALLINT ColumnNumber);
PyObject* get_datetime(Cursor *self, SQLUSMALLINT ColumnNumber);
PyObject* get_date(Cursor *self, SQLUSMALLINT ColumnNumber);
PyObject* get_time(Cursor *self, SQLUSMALLINT ColumnNumber);
PyObject* get_char(Cursor *self, SQLUSMALLINT ColumnNumber);
PyObject* get_wchar(Cursor *self, SQLUSMALLINT ColumnNumber);
PyObject* get_data(Cursor *self, SQLUSMALLINT column_number, SQLSMALLINT sql_type);

extern int check_error(PyObject *self, const char *fn_name);


#endif
