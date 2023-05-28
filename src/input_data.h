#ifndef _INPUT_DATA_H_
#define _INPUT_DATA_H_


#include "aodbc_types.h"
#include <datetime.h>


#ifdef __linux_
extern char16_t* wctouc(const wchar_t *wc);
#endif

int bind_null(Cursor *self, Py_ssize_t parameter_number, parameter *parameter_data);
int bind_integer(Cursor *self, Py_ssize_t parameter_number, PyObject *param, parameter *parameter_data);
int bind_bool(Cursor *self, Py_ssize_t parameter_number, PyObject *param, parameter *parameter_data);
int bind_float(Cursor *self, Py_ssize_t parameter_number, PyObject *param, parameter *parameter_data);
int bind_string(Cursor *self, Py_ssize_t parameter_number, PyObject *param, parameter *parameter_data);
int bind_datetime(Cursor *self, Py_ssize_t parameter_number, PyObject *param, parameter *parameter_data);
int bind_date(Cursor *self, Py_ssize_t parameter_number, PyObject *param, parameter *parameter_data);
int bind_time(Cursor *self, Py_ssize_t parameter_number, PyObject *param, parameter *parameter_data);
int bind_parameter(Cursor *self, Py_ssize_t parameter_number, PyObject *param, parameter *parameter_data);

extern int check_error(PyObject *self, const char *fn_name);


#endif
