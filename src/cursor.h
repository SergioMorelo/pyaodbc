#ifndef _CURSOR_H_
#define _CURSOR_H_


#include "aodbc_types.h"


PyTypeObject Cursor_Type;

void free_parameters(parameters_info *p_info);
int free_cursor(Cursor *self);
int allocate_cursor(Cursor *self, Connection *conn);
Py_ssize_t get_count_query_parameters(const wchar_t *query);
int check_parameters_equality(const wchar_t *query, Py_ssize_t params_length);

#ifdef __linux__
void* t_sql_exec_direct_w(void *handle);
extern short wait_for_single_object(t_event *event, unsigned long milliseconds);
extern char16_t* wctouc(const wchar_t *wc);
extern wchar_t* uctowc(char16_t *uc);
#endif

int prepare_execute(Cursor *self, const wchar_t *query, PyObject *params, Py_ssize_t params_length, long long timeout);

extern int check_error(PyObject *self, const char *fn_name);
extern int bind_parameter(Cursor *self, Py_ssize_t parameter_number, PyObject *param, parameter *parameter_data);
extern PyObject* get_data(Cursor *self, SQLUSMALLINT column_number, SQLSMALLINT sql_type);


#endif
