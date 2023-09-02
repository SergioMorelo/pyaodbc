#include "input_data.h"


int bind_null(Cursor *self, Py_ssize_t parameter_number, parameter *parameter_data)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    parameter_data->indicator = SQL_NULL_DATA;

    self->retcode = SQLBindParameter(
        self->handle,
        (SQLUSMALLINT)(parameter_number + 1),
        SQL_PARAM_INPUT,
        SQL_C_CHAR,
        SQL_VARCHAR,
        1,
        0,
        NULL,
        0,
        &parameter_data->indicator
    );
    CHECK_ERROR("bind_null::SQLBindParameter");

    return 0;
}


int bind_integer(Cursor *self, Py_ssize_t parameter_number, PyObject *param, parameter *parameter_data)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    long long value = PyLong_AsLongLong(param);
    SQLSMALLINT c_type;
    SQLSMALLINT sql_type;

    if (value < -2147483647 || value > 2147483647) {
        PRINT_DEBUG_MESSAGE("bind_integer::BIGINT");
        c_type = SQL_C_SBIGINT;
        sql_type = SQL_BIGINT;
        parameter_data->value.v_int64 = (INT64)value;
    } else {
        PRINT_DEBUG_MESSAGE("bind_integer::LONG");
        c_type = SQL_C_LONG;
        sql_type = SQL_INTEGER;
        parameter_data->value.v_int = (int)value;
    }

    self->retcode = SQLBindParameter(
        self->handle,
        (SQLUSMALLINT)(parameter_number + 1),
        SQL_PARAM_INPUT,
        c_type,
        sql_type,
        0,
        0,
        sql_type == SQL_BIGINT ? (SQLPOINTER)(&parameter_data->value.v_int64) : (SQLPOINTER)(&parameter_data->value.v_int),
        0,
        &parameter_data->indicator
    );
    CHECK_ERROR("bind_integer:SQLBindParameter");

    return 0;
}


int bind_bool(Cursor *self, Py_ssize_t parameter_number, PyObject *param, parameter *parameter_data)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    parameter_data->value.v_bool = (unsigned char)(param == Py_True ? 1 : 0);

    self->retcode = SQLBindParameter(
        self->handle,
        (SQLUSMALLINT)(parameter_number + 1),
        SQL_PARAM_INPUT,
        SQL_C_BIT,
        SQL_BIT,
        0,
        0,
        &parameter_data->value.v_bool,
        0,
        &parameter_data->indicator
    );
    CHECK_ERROR("bind_bool::SQLBindParameter");

    return 0;
}


int bind_float(Cursor *self, Py_ssize_t parameter_number, PyObject *param, parameter *parameter_data)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    parameter_data->value.v_float = PyFloat_AsDouble(param);

    self->retcode = SQLBindParameter(
        self->handle,
        (SQLUSMALLINT)(parameter_number + 1),
        SQL_PARAM_INPUT,
        SQL_C_DOUBLE,
        SQL_DOUBLE,
        0,
        0,
        &parameter_data->value.v_float,
        0,
        &parameter_data->indicator
    );
    CHECK_ERROR("bind_float::SQLBindParameter");

    return 0;
}


int bind_string(Cursor *self, Py_ssize_t parameter_number, PyObject *param, parameter *parameter_data)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    Py_ssize_t string_length = PyUnicode_GET_LENGTH(param);
    SQLSMALLINT sql_type = string_length > 2000 ? SQL_WLONGVARCHAR : SQL_WVARCHAR;

    wchar_t *w_value = PyUnicode_AsWideCharString(param, &string_length);
    if (w_value == NULL) {
        PyErr_NoMemory();
        return -1;
    }

    #ifdef _WIN32
    parameter_data->value.v_str = w_value;

    #elif __linux__
    char16_t *value = wctouc(w_value);
    if (value == NULL) {
        PyErr_NoMemory();
        return -1;
    }
    parameter_data->value.v_str = value;
    #endif

    parameter_data->alloc_str = 1;
    parameter_data->indicator = string_length * 2;  // * 2 because it's wchar_t

    self->retcode = SQLBindParameter(
        self->handle,
        (SQLUSMALLINT)(parameter_number + 1),
        SQL_PARAM_INPUT,
        SQL_C_WCHAR,
        sql_type,
        parameter_data->indicator,
        0,
        parameter_data->value.v_str,
        parameter_data->indicator,
        &parameter_data->indicator
    );
    CHECK_ERROR("bind_string::SQLBindParameter");

    return 0;
}


int bind_datetime(Cursor *self, Py_ssize_t parameter_number, PyObject *param, parameter *parameter_data)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    SQL_TIMESTAMP_STRUCT datetime;
    datetime.year = (SQLSMALLINT)PyDateTime_GET_YEAR(param);
    datetime.month = (SQLUSMALLINT)PyDateTime_GET_MONTH(param);
    datetime.day = (SQLUSMALLINT)PyDateTime_GET_DAY(param);
    datetime.hour = (SQLUSMALLINT)PyDateTime_DATE_GET_HOUR(param);
    datetime.minute = (SQLUSMALLINT)PyDateTime_DATE_GET_MINUTE(param);
    datetime.second = (SQLUSMALLINT)PyDateTime_DATE_GET_SECOND(param);
    int microseconds = (SQLUINTEGER)PyDateTime_DATE_GET_MICROSECOND(param);

    int decimal_digits;
    if (microseconds) {
        decimal_digits = 6;
        datetime.fraction = (SQLUINTEGER)(microseconds * 1000);
    } else {
        decimal_digits = 0;
        datetime.fraction = 0;
    }

    parameter_data->value.v_datetime = datetime;

    self->retcode = SQLBindParameter(
        self->handle,
        (SQLUSMALLINT)(parameter_number + 1),
        SQL_PARAM_INPUT,
        SQL_C_TYPE_TIMESTAMP,
        SQL_TYPE_TIMESTAMP,
        sizeof(SQL_TIMESTAMP_STRUCT),
        decimal_digits,
        &parameter_data->value.v_datetime,
        0,
        &parameter_data->indicator
    );
    CHECK_ERROR("bind_datetime::SQLBindParameter");

    return 0;
}


int bind_date(Cursor *self, Py_ssize_t parameter_number, PyObject *param, parameter *parameter_data)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    SQL_DATE_STRUCT date;
    date.year = (SQLSMALLINT)PyDateTime_GET_YEAR(param);
    date.month = (SQLUSMALLINT)PyDateTime_GET_MONTH(param);
    date.day = (SQLUSMALLINT)PyDateTime_GET_DAY(param);
    parameter_data->value.v_date = date;

    self->retcode = SQLBindParameter(
        self->handle,
        (SQLUSMALLINT)(parameter_number + 1),
        SQL_PARAM_INPUT,
        SQL_C_TYPE_DATE,
        SQL_TYPE_DATE,
        sizeof(SQL_DATE_STRUCT),
        0,
        &parameter_data->value.v_date,
        0,
        &parameter_data->indicator
    );
    CHECK_ERROR("bind_date::SQLBindParameter");

    return 0;
}


int bind_time(Cursor *self, Py_ssize_t parameter_number, PyObject *param, parameter *parameter_data)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    SQL_TIME_STRUCT time;
    time.hour = (SQLUSMALLINT)PyDateTime_TIME_GET_HOUR(param);
    time.minute = (SQLUSMALLINT)PyDateTime_TIME_GET_MINUTE(param);
    time.second = (SQLUSMALLINT)PyDateTime_TIME_GET_SECOND(param);
    parameter_data->value.v_time = time;

    self->retcode = SQLBindParameter(
        self->handle,
        (SQLUSMALLINT)(parameter_number + 1),
        SQL_PARAM_INPUT,
        SQL_C_TYPE_TIME,
        SQL_TYPE_TIME,
        sizeof(SQL_TIME_STRUCT),
        0,
        &parameter_data->value.v_time,
        0,
        &parameter_data->indicator
    );
    CHECK_ERROR("bind_time::SQLBindParameter");

    return 0;
}


int bind_parameter(Cursor *self, Py_ssize_t parameter_number, PyObject *param, parameter *parameter_data)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);
    
    PyDateTime_IMPORT;

    if (param == Py_None) {
        return bind_null(self, parameter_number, parameter_data);
    }

    // First We check the condition for bool, because bool is the subclass of integer
    if (PyBool_Check(param)) {
        return bind_bool(self, parameter_number, param, parameter_data);
    }

    if (PyLong_Check(param)) {
        return bind_integer(self, parameter_number, param, parameter_data);
    }

    if (PyFloat_Check(param)) {
        return bind_float(self, parameter_number, param, parameter_data);
    }

    if (PyUnicode_Check(param)) {
        return bind_string(self, parameter_number, param, parameter_data);
    }

    if (PyDateTime_Check(param)) {
        return bind_datetime(self, parameter_number, param, parameter_data);
    }

    if (PyDate_Check(param)) {
        return bind_date(self, parameter_number, param, parameter_data);
    }

    if (PyTime_Check(param)) {
        return bind_time(self, parameter_number, param, parameter_data);
    }

    // return bind_string(self, parameter_number, param, parameter_data);
    return -1;
}
