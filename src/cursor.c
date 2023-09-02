#include "headers.h"
#include "cursor.h"


// start static declarations
static PyObject* Cursor_Iter(Cursor *self);
static PyObject* Cursor_Next(Cursor *self);
static void Cursor_Dealloc(Cursor *self);
static PyAsyncMethods Cursor_Awaitable;
static PyObject* Cursor_Close(Cursor *self);
static PyObject* Cursor_Exit(Cursor *self, PyObject* args);
static PyObject* Cursor_Execute(Cursor *self, PyObject *args, PyObject *kwargs);
static PyObject* Cursor_Fetchall(Cursor *self);
// end static declarations


void free_parameters(parameters_info *p_info)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    if (p_info->parameters != NULL) {
        for (Py_ssize_t i = 0; i < p_info->params_length; i++) {
            if (p_info->parameters[i].alloc_str == 1) {
                PyMem_Free(p_info->parameters[i].value.v_str);
            }
        }
        free(p_info->parameters);
        p_info->parameters = NULL;
    }
}


int free_cursor(Cursor *self)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    if (self->conn->state != CONNECTED ) {
        PyErr_Format(PyExc_Exception, "(%s) The connection isn't established", __FUNCTION__);
        return -1;
    }

    if (self->state == TO_OPEN || self->state == CLOSED) {
        PyErr_Format(PyExc_Exception, "(%s) The cursor isn't opened", __FUNCTION__);
        return -1;
    }

    if (self->state == OPENED || self->state == EXECUTED) {
        free_parameters(&self->p_info);

        self->retcode = SQLFreeHandle(SQL_HANDLE_STMT, self->handle);
        CHECK_ERROR("free_cursor::SQLFreeHandle");

        self->handle = SQL_NULL_HSTMT;
        self->retcode = -1;
        self->state = CLOSED;

        PY_MEM_FREE_TO_NULL(self->query);

        return 0;
    }

    if (self->state == TO_EXECUTE) {
        PyErr_Format(PyExc_Exception, "(%s) Canceling of the executed instruction isn't implemented", __FUNCTION__);
        return -1;
    }

    PyErr_Format(PyExc_Exception, "(%s) An undefined cursor state", __FUNCTION__);
    return -1;
}


int allocate_cursor(Cursor *self, Connection *conn)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    self->conn = NULL;
    self->handle = SQL_NULL_HSTMT;
    self->handle_type = SQL_HANDLE_STMT;
    self->retcode = -1;
    self->event = NULL;
    self->event_status = 258;
    self->state = CLOSED;
    self->p_info.parameters = NULL;
    self->p_info.params_length = 0;
    self->query = NULL;
    self->timeout = 0;
    self->start_time = 0;

    self->retcode = SQLAllocHandle(SQL_HANDLE_STMT, conn->handle, &self->handle);
    CHECK_ERROR("allocate_cursor::SQLAllocHandle::SQL_HANDLE_STMT");

    self->conn = conn;
    return 0;
}


static PyObject* Cursor_Iter(Cursor *self)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    Py_INCREF(self);
    return (PyObject *)self;
}


static PyObject* Cursor_Next(Cursor *self)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    if (self->conn->state != CONNECTED ) {
        PyErr_Format(PyExc_Exception, "(%s) The connection isn't established", __FUNCTION__);
        return NULL;
    }

    if (self->state == EXECUTED) {
        PyErr_Format(PyExc_Exception, "(%s) The cursor is already executed, You need to take results", __FUNCTION__);
        return NULL;
    }

    if (self->state == CLOSED) {
        PyErr_Format(PyExc_Exception, "(%s) The cursor is already closed", __FUNCTION__);
        return NULL;
    }

    if (self->state == TO_EXECUTE) {
        if (self->event_status != WAIT_OBJECT_0) {
            if (self->start_time && \
                clock() / CLOCKS_PER_SEC - self->start_time >= self->timeout + (long long)(1 / self->conn->rate)) {
                self->event_status = WAIT_OBJECT_0;
                
                PyErr_Format(PyExc_SystemError, "(%s) An event error", __FUNCTION__);
                return NULL;
            }

            #ifdef _WIN32
            self->event_status = WaitForSingleObject(self->event, (DWORD)(50 * self->conn->rate));

            #elif __linux__
            self->event_status = wait_for_single_object(self->event, (unsigned long)(50 * self->conn->rate));
            #endif

            Py_RETURN_NONE;
        }
        self->state = EXECUTED;
        free_parameters(&self->p_info);

        #ifdef _WIN32
        SQLCompleteAsync(self->handle_type, self->handle, &self->retcode);
        #endif
        PRINT_DEBUG_MESSAGE("SQLCompleteAsync");

        if (self->event != NULL) {
            #ifdef _WIN32
            CloseHandle(self->event);
            PRINT_DEBUG_MESSAGE("Cursor_Next::Close Handle");

            #elif __linux__
            close_t_event(self->event);
            PRINT_DEBUG_MESSAGE("Cursor_Next::close_t_event Handle");
            #endif
            
            self->event = NULL;
            self->event_status = 258;
        }

        if (check_error((PyObject *)self, "Cursor_Next::SQLCompleteAsync")) {
            return NULL;
        }

        PyErr_SetObject(PyExc_StopIteration, (PyObject *)self);
        return NULL;
    }

    PyErr_Format(PyExc_TypeError, "(%s) A coroutine was expected", __FUNCTION__);
    return NULL;
}


static void Cursor_Dealloc(Cursor *self)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    if (self->state == OPENED || self->state == EXECUTED) {
        free_cursor(self);
    }

    Py_CLEAR(self->conn);
    PyObject_Del(self);
}


static PyAsyncMethods Cursor_Awaitable = {
    .am_await = (unaryfunc)Cursor_Iter
};


static PyObject* Cursor_Close(Cursor *self)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    if (free_cursor(self) == -1) {
        return NULL;
    }

    Py_RETURN_NONE;
}


static PyObject* Cursor_Exit(Cursor *self, PyObject* args)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    PyObject *none = Cursor_Close(self);

    PyObject *type, *value, *traceback;
    if (PyArg_ParseTuple(args, "OOO", &type, &value, &traceback) && type != Py_None) {
        return NULL;
    }

    return none;
}


Py_ssize_t get_count_query_parameters(const wchar_t *query)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);
    
    Py_ssize_t count = 0;
    size_t string_length = wcslen(query);
    for (size_t i = 0; i < string_length; i++) {
        if (query[i] == '?') {
            count++;
        }
    }
    return count;
}


int check_parameters_equality(const wchar_t *query, Py_ssize_t params_length)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);
    
    Py_ssize_t count_query_parameters = get_count_query_parameters(query);

    if (count_query_parameters != params_length) {
        PyErr_Format(
            PyExc_TypeError,
            "(%s) The query takes %lld arguments, but %lld were given",
            __FUNCTION__, count_query_parameters, (long long)params_length
        );
        return -1;
    }

    return 0;
}


#ifdef __linux__
void* t_sql_exec_direct_w(void *handle)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);
    
    HANDLE event = (HANDLE)handle;

    Cursor *cursor = event->obj;

    char16_t *query = wctouc(cursor->query);
    if (query == NULL) {
        cursor->retcode = -1;
        goto clean_up;
    }

    cursor->retcode = SQLExecDirectW(cursor->handle, (SQLWCHAR *)query, SQL_NTS);
    free(query);

    clean_up:
        event->state = WAIT_OBJECT_0;
        pthread_exit(0);
}
#endif


int prepare_execute(Cursor *self, const wchar_t *query, PyObject *params, Py_ssize_t params_length, long long timeout)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    if (self->conn->state != CONNECTED ) {
        PyErr_Format(PyExc_Exception, "(%s) The connection isn't established", __FUNCTION__);
        return -1;
    }

    if (self->state == TO_OPEN || self->state == CLOSED) {
        PyErr_Format(PyExc_Exception, "(%s) The cursor isn't opened", __FUNCTION__);
        return -1;
    }

    if (self->conn->runned_cursors >= self->conn->mca) {
        PyErr_Format(
            PyExc_Exception,
            "(%s) The number of cursors from one connection can't exceed max concurrent activities (%d)",
            __FUNCTION__, self->conn->mca
        );
        return -1;
    }

    parameter *parameters = NULL;
    if (params_length) {
        parameters = (parameter *)malloc(sizeof(parameter) * params_length);
        if (parameters == NULL) {
            PyErr_NoMemory();
            return -1;
        }

        for (Py_ssize_t i = 0; i < params_length; i++) {
            parameters[i].alloc_str = 0;
            parameters[i].indicator = SQL_NTS;
            PyObject *param = PyTuple_GetItem(params, i);
            if (bind_parameter(self, i, param, &parameters[i]) == -1) {
                if (!PyErr_Occurred()) {
                    PyErr_Format(
                        PyExc_TypeError,
                        "(%s) The parameter type %lld in the query isn't supported for passing to SQL",
                        __FUNCTION__, i + 1
                    );
                }
                return -1;
            }
        }
    }
    self->p_info.parameters = parameters;
    self->p_info.params_length = params_length;
    self->query = query;
    self->timeout = timeout;

    self->retcode = SQLSetStmtAttr(self->handle, SQL_ATTR_QUERY_TIMEOUT, (SQLPOINTER)self->timeout, SQL_IS_INTEGER);
    CHECK_ERROR("prepare_execute::SQLSetStmtAttr::SQL_ATTR_QUERY_TIMEOUT ");

    #ifdef _WIN32
    self->retcode = SQLSetStmtAttr(self->handle, SQL_ATTR_ASYNC_ENABLE, (SQLPOINTER)SQL_ASYNC_ENABLE_ON, SQL_IS_INTEGER);
    CHECK_ERROR("prepare_execute::SQLSetStmtAttr::SQL_ATTR_ASYNC_ENABLE");

    self->event = CreateEvent(NULL, FALSE, FALSE, NULL);
    self->event_status = 258;
    CHECK_EVENT_ERROR(self->event, "prepare_execute::CreateEvent");

    self->retcode = SQLSetStmtAttr(self->handle, SQL_ATTR_ASYNC_STMT_EVENT, self->event, SQL_IS_POINTER);
    CHECK_ERROR("prepare_execute::SQLSetStmtAttr::SQL_ATTR_ASYNC_STMT_EVENT");

    self->retcode = SQLExecDirectW(self->handle, (SQLWCHAR *)self->query, SQL_NTS);
    CHECK_ERROR("prepare_execute::SQLExecDirectW");

    #elif __linux__
    self->event = create_t_event();
    CHECK_EVENT_ERROR(self->event, "prepare_execute::create_t_event");

    self->event->obj = self;

    pthread_create(&self->event->thread, NULL, t_sql_exec_direct_w, self->event);
    pthread_detach(self->event->thread);
    #endif

    self->conn->runned_cursors++;
    self->state = TO_EXECUTE;
    if (timeout) {
        self->start_time = clock() / CLOCKS_PER_SEC;
    }
    return 0;
}


static PyObject* Cursor_Execute(Cursor *self, PyObject *args, PyObject *kwargs)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    static char *kwlist[] = {"query", "params", "timeout", NULL};
    PyObject *py_query = NULL;
    PyObject *params = NULL;
    long long timeout = 0;
    const wchar_t *query;
    Py_ssize_t string_length;
    Py_ssize_t params_length = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|OL", kwlist, &py_query, &params, &timeout)) {
        return NULL;
    }

    if (!PyUnicode_Check(py_query)) {
        PyErr_Format(PyExc_AttributeError, "(%s) The query must be an Unicode string", __FUNCTION__);
        return NULL;
    }

    string_length = PyUnicode_GET_LENGTH(py_query);
    query = PyUnicode_AsWideCharString(py_query, &string_length);
    if (query == NULL) {
        PyErr_NoMemory();
        return NULL;
    }

    if (timeout < 0) {
        PyErr_Format(PyExc_AttributeError, "(%s) The value must be nonnegative", __FUNCTION__);
        return NULL;
    }

    if (timeout > 2147483647) {
        PyErr_Format(PyExc_AttributeError, "(%s) The value must be less than 2147483648", __FUNCTION__);
        return NULL;
    }

    if (params && params != Py_None) {
        if (!PyTuple_Check(params)) {
            PyErr_Format(PyExc_TypeError, "(%s) Params must be in a tuple", __FUNCTION__);
            return NULL;
        } else {
            Py_INCREF(params);
            params_length = PyTuple_GET_SIZE(params);
        }
    }

    if (check_parameters_equality(query, params_length) == -1) {
        Py_XDECREF(params);
        PyMem_Free((void *)query);
        return NULL;
    }

    if (prepare_execute((Cursor *)self, query, params, params_length, timeout) == -1) {
        Py_XDECREF(params);
        PY_MEM_FREE_TO_NULL(self->query);

        return NULL;
    }
    Py_XDECREF(params);

    Py_INCREF(self);
    return (PyObject *)self;
}


static PyObject* Cursor_Fetchall(Cursor *self)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    SQLSMALLINT column_count;
    SQLWCHAR column_name[256];
    SQLSMALLINT name_length;
    SQLSMALLINT sql_type;
    SQLULEN column_size;
    SQLSMALLINT decimal_digits;
    SQLSMALLINT nullable;

    PyObject *results = NULL;
    PyObject *row = NULL;
    PyObject *value = NULL;
    PyObject *key = NULL;

    wchar_t *w_column_name;

    if (self->conn->state != CONNECTED ) {
        PyErr_Format(PyExc_Exception, "(%s) The connection isn't established", __FUNCTION__);
        goto clean_up;
    }

    if (self->state == TO_OPEN || self->state == CLOSED) {
        PyErr_Format(PyExc_Exception, "(%s) The cursor isn't opened", __FUNCTION__);
        goto clean_up;
    }

    if (self->state != EXECUTED) {
        PyErr_Format(PyExc_Exception, "(%s) The cursor wasn't executed", __FUNCTION__);
        goto clean_up;
    }

    self->retcode = SQLNumResultCols(self->handle, &column_count);
    if (check_error((PyObject *)self, "Cursor_Fetchall::SQLNumResultCols")) {
        goto clean_up;
    }

    results = PyList_New(0);
    if (results == NULL) {
        PyErr_Format(PyExc_Exception, "(%s) Failed to create List", __FUNCTION__);
        goto clean_up;
    }

    while (SQL_SUCCEEDED(SQLFetch(self->handle))) {
        row = PyDict_New();
        if (row == NULL) {
            PyErr_Format(PyExc_Exception, "(%s) Failed to create Dict", __FUNCTION__);
            goto clean_up;
        }

        for (SQLUSMALLINT i = 0; i < column_count; i++) {
            self->retcode = SQLDescribeColW(
                self->handle,
                (SQLUSMALLINT)(i + 1),
                column_name,
                sizeof(column_name),
                &name_length,
                &sql_type,
                &column_size,
                &decimal_digits,
                &nullable
            );

            if (check_error((PyObject *)self, "(%s) Cursor_Fetchall::SQLDescribeColW")) {
                goto clean_up;
            }

            value = get_data(self, i, sql_type);
            if (value == NULL) {
                goto clean_up;
            }

            #ifdef _WIN32
            w_column_name = (wchar_t *)malloc(sizeof(column_name));
            if (w_column_name == NULL) {
                PyErr_NoMemory();
                goto clean_up;
            }
            wcscpy(w_column_name, (wchar_t *)column_name);

            #elif __linux__
            w_column_name = uctowc(column_name);
            if (w_column_name == NULL) {
                PyErr_NoMemory();
                goto clean_up;
            }
            #endif

            key = PyUnicode_FromWideChar(w_column_name, -1);
            free(w_column_name);

            if (key == NULL) {
                PyErr_Format(PyExc_Exception, "(%s) Failed to get FieldName", __FUNCTION__);
                goto clean_up;
            }

            if (PyDict_SetItem(row, key, value) == -1) {
                PyErr_Format(PyExc_Exception, "(%s) Failed to set KeyValue", __FUNCTION__);
                goto clean_up;
            }
            Py_DECREF(key);
            Py_DECREF(value);
        }

        if (PyList_Append(results, row) == -1) {
            PyErr_Format(PyExc_Exception, "(%s) Failed to add a element into List", __FUNCTION__);
            goto clean_up;
        }
        Py_DECREF(row);
    }

    clean_up:
        self->conn->runned_cursors--;
        self->state = OPENED;

        if (PyErr_Occurred()) {
            Py_XDECREF(key);
            Py_XDECREF(value);
            Py_XDECREF(row);
            Py_XDECREF(results);
            return NULL;
        } else {
            return results;
        }
}


static PyMethodDef Cursor_Methods[] = {
    {"__enter__", (PyCFunction)Cursor_Iter, METH_NOARGS, "Open a cursor"},
    {"__exit__", (PyCFunction)Cursor_Exit, METH_VARARGS, "Close a cursor"},
    {"execute", (PyCFunction)Cursor_Execute, METH_VARARGS|METH_KEYWORDS, "Asynchronous execution"},
    {"fetchall", (PyCFunction)Cursor_Fetchall, METH_NOARGS, "Fetchall results"},
    {"close", (PyCFunction)Cursor_Close, METH_NOARGS, "Close a cursor"},
    {NULL, NULL, 0, NULL}
};


PyTypeObject Cursor_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "pyaodbc.Cursor",
    .tp_doc = PyDoc_STR("Asynchronous Cursor Class"),
    .tp_basicsize = sizeof(Cursor),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_iter = (getiterfunc)Cursor_Iter,
    .tp_iternext = (iternextfunc)Cursor_Next,
    .tp_dealloc = (destructor)Cursor_Dealloc,
    .tp_as_async = &Cursor_Awaitable,
    .tp_methods = Cursor_Methods
};
