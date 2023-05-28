#include "get_data.h"


PyObject* get_integer_smallint(Cursor *self, SQLUSMALLINT column_number)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    SQLLEN is_unsigned;
    SQLLEN len_or_indicator;
    SQLINTEGER value;
    SQLSMALLINT target_type;

    self->retcode = SQLColAttribute(
        self->handle,
        (SQLUSMALLINT)(column_number + 1),
        SQL_DESC_UNSIGNED,
        0,
        0,
        0,
        &is_unsigned
    );
    if (check_error((PyObject *)self, "get_integer_smallint::SQLColAttribute")) {
        return NULL;
    }

    target_type = is_unsigned ? SQL_C_ULONG : SQL_C_LONG;

    self->retcode = SQLGetData(
        self->handle,
        (SQLUSMALLINT)(column_number + 1),
        target_type,
        &value,
        sizeof(value),
        &len_or_indicator
    );
    if (check_error((PyObject *)self, "get_integer_smallint::SQLGetData")) {
        return NULL;
    }

    if (len_or_indicator == SQL_NULL_DATA) {
        Py_RETURN_NONE;
    } else if (is_unsigned) {
        return PyLong_FromUnsignedLong((unsigned long)value);
    } else {
        return PyLong_FromLong(value);
    }
}


PyObject* get_bigint(Cursor *self, SQLUSMALLINT column_number)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    SQLLEN is_unsigned;
    SQLLEN len_or_indicator;
    SQLBIGINT value;
    SQLSMALLINT target_type;

    self->retcode = SQLColAttribute(
        self->handle,
        (SQLUSMALLINT)(column_number + 1),
        SQL_DESC_UNSIGNED,
        0,
        0,
        0,
        &is_unsigned
    );
    if (check_error((PyObject *)self, "get_bigint::SQLColAttribute")) {
        return NULL;
    }

    target_type = is_unsigned ? SQL_C_UBIGINT : SQL_C_SBIGINT;

    self->retcode = SQLGetData(
        self->handle,
        (SQLUSMALLINT)(column_number + 1),
        target_type,
        &value,
        sizeof(value),
        &len_or_indicator
    );
    if (check_error((PyObject *)self, "get_bigint::SQLGetData")) {
        return NULL;
    }

    if (len_or_indicator == SQL_NULL_DATA) {
        Py_RETURN_NONE;
    } else if (is_unsigned) {
        return PyLong_FromUnsignedLongLong((unsigned PY_LONG_LONG)(SQLUBIGINT)value);
    } else {
        return PyLong_FromLongLong((PY_LONG_LONG)value);
    }
}


PyObject* get_numeric_decimal(Cursor *self, SQLUSMALLINT column_number)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    SQLHDESC desc = SQL_NULL_HDESC;
    SQLLEN len_or_indicator;
    SQL_NUMERIC_STRUCT sql_numeric;
    SQLLEN scale;
    SQLLEN precision;

    double final_value;
    double divisor;
    int sign = 1;
    int last = 1;
    int remainder;
    int integer;
    int current;
    long value = 0;

    self->retcode = SQLGetStmtAttr(self->handle, SQL_ATTR_APP_ROW_DESC, &desc, 0, NULL);
    if (check_error((PyObject *)self, "get_numeric_decimal::SQLGetStmtAttr")) {
        return NULL;
    }

    self->retcode = SQLColAttribute(
        self->handle,
        (SQLUSMALLINT)(column_number + 1),
        SQL_DESC_SCALE,
        0,
        0,
        0,
        &scale
    );
    if (check_error((PyObject *)self, "get_numeric_decimal::SQLColAttribute::SQL_DESC_SCALE")) {
        return NULL;
    }

    self->retcode = SQLColAttribute(
        self->handle,
        (SQLUSMALLINT)(column_number + 1),
        SQL_DESC_PRECISION,
        0,
        0,
        0,
        &precision
    );
    if (check_error((PyObject *)self, "get_numeric_decimal::SQLColAttribute::SQL_DESC_PRECISION")) {
        return NULL;
    }

    self->retcode = SQLSetDescField(desc, (SQLSMALLINT)(column_number + 1), SQL_DESC_TYPE, (void *)SQL_C_NUMERIC, 0);
    if (check_error((PyObject *)self, "get_numeric_decimal::SQLSetDescField::SQL_DESC_TYPE")) {
        return NULL;
    }

    self->retcode = SQLSetDescField(desc, (SQLSMALLINT)(column_number + 1), SQL_DESC_PRECISION, (void *)precision, 0);
    if (check_error((PyObject *)self, "get_numeric_decimal::SQLSetDescField::SQL_DESC_PRECISION")) {
        return NULL;
    }

    self->retcode = SQLSetDescField(desc, (SQLSMALLINT)(column_number + 1), SQL_DESC_SCALE, (void *)scale, 0);
    if (check_error((PyObject *)self, "get_numeric_decimal::SQLSetDescField::SQL_DESC_SCALE")) {
        return NULL;
    }

    self->retcode = SQLGetData(
        self->handle,
        (SQLUSMALLINT)(column_number + 1),
        SQL_ARD_TYPE,
        &sql_numeric,
        sizeof(sql_numeric),
        &len_or_indicator
    );
    if (check_error((PyObject *)self, "get_numeric_decimal::SQLGetData")) {
        return NULL;
    }

    if (len_or_indicator == SQL_NULL_DATA) {
        Py_RETURN_NONE;
    } else {
        for (int i = 0; i < SQL_MAX_NUMERIC_LEN; i++) {
            current = (int)sql_numeric.val[i];
            remainder = current % 16;
            integer = current / 16;

            value += last * remainder;
            last *= 16;
            value += last * integer;
            last *= 16;
        }

        divisor = pow(10, (double)sql_numeric.scale);
        final_value = (double)value / divisor;
        
        if (!sql_numeric.sign) {
            sign = -1;
        }
        final_value *= sign;
        
        return PyFloat_FromDouble(final_value);
    } 
}


PyObject* get_bit(Cursor *self, SQLUSMALLINT column_number)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    SQLLEN len_or_indicator;
    SQLCHAR value;

    self->retcode = SQLGetData(
        self->handle,
        (SQLUSMALLINT)(column_number + 1),
        SQL_C_BIT,
        &value,
        sizeof(value),
        &len_or_indicator
    );

    if (check_error((PyObject *)self, "get_bit::SQLGetData")) {
        return NULL;
    }

    if (len_or_indicator == SQL_NULL_DATA) {
        Py_RETURN_NONE;
    } else {
        return PyBool_FromLong((long)value);
    }
}


PyObject* get_tinyint(Cursor *self, SQLUSMALLINT column_number)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    SQLLEN is_unsigned;
    SQLLEN len_or_indicator;
    SQLCHAR value;
    SQLSMALLINT target_type;
    
    self->retcode = SQLColAttribute(
        self->handle,
        (SQLUSMALLINT)(column_number + 1),
        SQL_DESC_UNSIGNED,
        0,
        0,
        0,
        &is_unsigned
    );
    if (check_error((PyObject *)self, "get_tinyint::SQLColAttribute")) {
        return NULL;
    }

    target_type = is_unsigned ? SQL_C_UTINYINT : SQL_C_STINYINT;

    self->retcode = SQLGetData(
        self->handle,
        (SQLUSMALLINT)(column_number + 1),
        target_type,
        &value,
        sizeof(value),
        &len_or_indicator
    );

    if (check_error((PyObject *)self, "get_tinyint::SQLGetData")) {
        return NULL;
    }

    if (len_or_indicator == SQL_NULL_DATA) {
        Py_RETURN_NONE;
    } else if (is_unsigned) {
        return PyLong_FromUnsignedLong((unsigned long)value);
    } else {
        return PyLong_FromLong((long)value);
    }
}


PyObject* get_float(Cursor *self, SQLUSMALLINT column_number)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    SQLLEN len_or_indicator;
    SQLREAL value;

    self->retcode = SQLGetData(
        self->handle,
        (SQLUSMALLINT)(column_number + 1),
        SQL_C_FLOAT,
        &value,
        sizeof(value),
        &len_or_indicator
    );

    if (check_error((PyObject *)self, "get_float::SQLGetData")) {
        return NULL;
    }

    if (len_or_indicator == SQL_NULL_DATA) {
        Py_RETURN_NONE;
    } else {
        return PyFloat_FromDouble((double)value);
    }
}


PyObject* get_float_real_double(Cursor *self, SQLUSMALLINT column_number)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    SQLLEN len_or_indicator;
    SQLDOUBLE value;

    self->retcode = SQLGetData(
        self->handle,
        (SQLUSMALLINT)(column_number + 1),
        SQL_C_DOUBLE,
        &value,
        sizeof(value),
        &len_or_indicator
    );

    if (check_error((PyObject *)self, "get_float_real_double::SQLGetData")) {
        return NULL;
    }

    if (len_or_indicator == SQL_NULL_DATA) {
        Py_RETURN_NONE;
    } else {
        return PyFloat_FromDouble(value);
    }
}

PyObject* get_datetime(Cursor *self, SQLUSMALLINT column_number)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    SQLLEN len_or_indicator;
    SQL_TIMESTAMP_STRUCT datetime;

    self->retcode = SQLGetData(
        self->handle,
        (SQLUSMALLINT)(column_number + 1),
        SQL_C_TYPE_TIMESTAMP,
        &datetime,
        sizeof(datetime),
        &len_or_indicator
    );

    if (check_error((PyObject *)self, "get_datetime::SQLGetData")) {
        return NULL;
    }

    if (len_or_indicator == SQL_NULL_DATA) {
        Py_RETURN_NONE;
    } else {
        return PyDateTime_FromDateAndTime(
            (int)datetime.year,
            (int)datetime.month,
            (int)datetime.day,
            (int)datetime.hour,
            (int)datetime.minute,
            (int)datetime.second,
            (int)(datetime.fraction / 1000)
        );
    }
}


PyObject* get_date(Cursor *self, SQLUSMALLINT column_number)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    SQLLEN len_or_indicator;
    SQL_DATE_STRUCT date;

    self->retcode = SQLGetData(
        self->handle,
        (SQLUSMALLINT)(column_number + 1),
        SQL_C_TYPE_DATE,
        &date,
        sizeof(date),
        &len_or_indicator
    );

    if (check_error((PyObject *)self, "get_date::SQLGetData")) {
        return NULL;
    }

    if (len_or_indicator == SQL_NULL_DATA) {
        Py_RETURN_NONE;
    } else {
        return PyDate_FromDate((int)date.year, (int)date.month, (int)date.day);
    }
}


PyObject* get_time(Cursor *self, SQLUSMALLINT column_number)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    SQLLEN len_or_indicator;
    SQL_TIME_STRUCT time;

    self->retcode = SQLGetData(
        self->handle,
        (SQLUSMALLINT)(column_number + 1),
        SQL_C_TYPE_TIME,
        &time,
        sizeof(time),
        &len_or_indicator
    );

    if (check_error((PyObject *)self, "get_time::SQLGetData")) {
        return NULL;
    }

    if (len_or_indicator == SQL_NULL_DATA) {
        Py_RETURN_NONE;
    } else {
        return PyTime_FromTime((int)time.hour, (int)time.minute, (int)time.second, 0);
    }
}


PyObject* get_char(Cursor *self, SQLUSMALLINT column_number)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    SQLLEN len_or_indicator;
    size_t allocated = 4096;  // bytes
    size_t readed = 0;
    size_t index = 0;
    size_t null_terminated = sizeof(SQLCHAR);
    long long needed;

    SQLCHAR *buffer = (SQLCHAR *)malloc(allocated + null_terminated);
    if (buffer == NULL) {
        PyErr_NoMemory();
        return NULL;
    }

    size_t can_read = allocated - readed;

    self->retcode = SQLGetData(
        self->handle,
        (SQLUSMALLINT)(column_number + 1),
        SQL_C_CHAR,
        &buffer[index],
        (SQLLEN)can_read,
        &len_or_indicator
    );

    readed += can_read - null_terminated;

    if (!SQL_SUCCEEDED(self->retcode) && self->retcode != SQL_SUCCESS_WITH_INFO && \
        self->retcode != SQL_STILL_EXECUTING && self->retcode != SQL_NO_DATA) {
        PyErr_SetString(PyExc_Exception, "(get_char::SQLGetData) A failed retcode at getting a data");
        free(buffer);
        return NULL;
    }

    if (self->retcode == SQL_SUCCESS && len_or_indicator == SQL_NULL_DATA) {
        free(buffer);
        Py_RETURN_NONE;
    }

    if (self->retcode == SQL_SUCCESS && len_or_indicator + null_terminated < allocated) {
        buffer = (SQLCHAR *)realloc(buffer, len_or_indicator + null_terminated);
        if (buffer == NULL) {
            PyErr_NoMemory();
            free(buffer);
            return NULL;
        }
    }

    index += can_read / sizeof(SQLCHAR) - 1;  // a shift of -1, because a string ends with '\0'

    while (self->retcode == SQL_SUCCESS_WITH_INFO) {
        needed = len_or_indicator - readed;
        if (needed < 0) {
            break;
        }

        allocated += needed;
        can_read = allocated - readed;

        buffer = (SQLCHAR *)realloc(buffer, allocated);
        if (buffer == NULL) {
            PyErr_NoMemory();
            free(buffer);
            return NULL;
        }

        self->retcode = SQLGetData(
            self->handle,
            (SQLUSMALLINT)(column_number + 1),
            SQL_C_CHAR,
            &buffer[index],
            (SQLLEN)can_read,
            &len_or_indicator
        );

        readed += can_read - null_terminated;
        if (self->retcode == SQL_SUCCESS) {
            break;
        }

        index += can_read / sizeof(SQLCHAR) - 1;
        if (self->retcode == SQL_NO_DATA) {
            break;
        }
    }

    PyObject *value = Py_BuildValue("s", buffer);
    free(buffer);

    if (value == NULL) {
        PyErr_SetString(PyExc_Exception, "(get_char::Py_BuildValue) An exception at building a value");
        return NULL;
    }
    
    return value;
}


PyObject* get_wchar(Cursor *self, SQLUSMALLINT column_number)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    SQLLEN len_or_indicator;
    size_t allocated = 4096;  // bytes
    size_t readed = 0;
    size_t index = 0;
    size_t null_terminated = sizeof(SQLWCHAR);
    long long needed;

    SQLWCHAR *buffer = (SQLWCHAR *)malloc(allocated + null_terminated);
    if (buffer == NULL) {
        PyErr_NoMemory();
        return NULL;
    }

    size_t can_read = allocated - readed;

    self->retcode = SQLGetData(
        self->handle,
        (SQLUSMALLINT)(column_number + 1),
        SQL_C_WCHAR,
        &buffer[index],
        (SQLLEN)can_read,
        &len_or_indicator
    );

    readed += can_read - null_terminated;

    if (!SQL_SUCCEEDED(self->retcode) && self->retcode != SQL_SUCCESS_WITH_INFO && \
        self->retcode != SQL_STILL_EXECUTING && self->retcode != SQL_NO_DATA) {
        PyErr_SetString(PyExc_Exception, "(get_wchar::SQLGetData) A failed retcode at getting a data");
        free(buffer);
        return NULL;
    }

    if (self->retcode == SQL_SUCCESS && len_or_indicator == SQL_NULL_DATA) {
        free(buffer);
        Py_RETURN_NONE;
    }

    if (self->retcode == SQL_SUCCESS && len_or_indicator + null_terminated < allocated) {
        buffer = (SQLWCHAR *)realloc(buffer, len_or_indicator + null_terminated);
        if (buffer == NULL) {
            PyErr_NoMemory();
            free(buffer);
            return NULL;
        }
    }

    index += can_read / sizeof(SQLWCHAR) - 1;  // a shift of -1, because a string ends with '\0'

    while (self->retcode == SQL_SUCCESS_WITH_INFO) {
        needed = len_or_indicator - readed;
        if (needed < 0) {
            break;
        }

        allocated += needed;
        can_read = allocated - readed;

        buffer = (SQLWCHAR *)realloc(buffer, allocated);
        if (buffer == NULL) {
            PyErr_NoMemory();
            free(buffer);
            return NULL;
        }

        self->retcode = SQLGetData(
            self->handle,
            (SQLUSMALLINT)(column_number + 1),
            SQL_C_WCHAR,
            &buffer[index],
            (SQLLEN)can_read,
            &len_or_indicator
        );

        readed += can_read - null_terminated;
        if (self->retcode == SQL_SUCCESS) {
            break;
        }

        index += can_read / sizeof(SQLWCHAR) - 1;
        if (self->retcode == SQL_NO_DATA) {
            break;
        }
    }


    #ifdef _WIN32
    PyObject *value = PyUnicode_FromWideChar((wchar_t *)buffer, -1);
    free(buffer);
    
    #elif __linux__
    wchar_t *w_buffer = uctowc(buffer);
    free(buffer);
    if (w_buffer == NULL) {
        PyErr_NoMemory();
        return NULL;
    }
    PyObject *value = PyUnicode_FromWideChar(w_buffer, -1);
    free(w_buffer);
    #endif

    if (value == NULL) {
        PyErr_SetString(PyExc_Exception, "(get_wchar::PyUnicode_FromWideChar) An exception at building a value");
        return NULL;
    }

    return value;
}


PyObject* get_data(Cursor *self, SQLUSMALLINT column_number, SQLSMALLINT sql_type)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);
    
    PyDateTime_IMPORT;

    switch (sql_type) {
        case SQL_INTEGER:
        case SQL_SMALLINT:
            return get_integer_smallint(self, column_number);
        case SQL_BIGINT:
            return get_bigint(self, column_number);
        case SQL_NUMERIC:
        case SQL_DECIMAL:
            return get_numeric_decimal(self, column_number);
        case SQL_BIT:
            return get_bit(self, column_number);
        case SQL_TINYINT:
            return get_tinyint(self, column_number);
        case SQL_FLOAT:
        case SQL_REAL:
        case SQL_DOUBLE:
            return get_float_real_double(self, column_number);
        case SQL_TYPE_TIMESTAMP:
            return get_datetime(self, column_number);
        case SQL_TYPE_DATE:
            return get_date(self, column_number);
        case SQL_TYPE_TIME:
        case -154:  // SQL Server 2008+
            return get_time(self, column_number);
        case SQL_CHAR:
        case SQL_VARCHAR:
        case SQL_LONGVARCHAR:
            return get_wchar(self, column_number);  // TODO to convert UTF-16 (surrogate pairs and e.t.c)?
        case -155:  // datetimeoffset
            return get_char(self, column_number);
        case SQL_WCHAR:
        case SQL_WVARCHAR:
        case SQL_WLONGVARCHAR:
            return get_wchar(self, column_number);
        default:
            PRINT_DEBUG_MESSAGE("return default!");
            return get_wchar(self, column_number);
    }
}
