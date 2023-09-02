#include "headers.h"
#include "connection.h"


// start static declarations
static PyObject* Connection_Iter(Connection *self);
static PyObject* Connection_Next(Connection *self);
static void Connection_Dealloc(Connection *self);
static PyAsyncMethods Connection_Awaitable;
static PyObject* Connection_Cursor(Connection *self);
static PyObject* Connection_Close(Connection *self);
static PyObject* Connection_Aexit(Connection *self, PyObject* args);
// end static declarations


#ifdef __linux__
void* t_sql_driver_connect_w(void *handle)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    HANDLE event = (HANDLE)handle;

    SQLWCHAR out_conn_str[1024];
    SQLSMALLINT out_conn_str_len;
    const wchar_t *w_dsn = event->str;
    Connection *conn = event->obj;

    char16_t *dsn = wctouc(w_dsn);
    if (dsn == NULL) {
        PyErr_NoMemory();
        conn->retcode = -1;
        goto clean_up;
    }

    conn->retcode = SQLDriverConnectW(
        conn->handle,
        NULL,
        (SQLWCHAR *)dsn,
        SQL_NTS,
        out_conn_str,
        0,
        &out_conn_str_len,
        SQL_DRIVER_NOPROMPT
    );
    free(dsn);

    clean_up:
        event->state = WAIT_OBJECT_0;
        pthread_exit(0);
}


void* t_sql_disconnect(void *handle)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    HANDLE event = (HANDLE)handle;

    Connection *conn = event->obj;
    conn->retcode = SQLDisconnect(conn->handle);

    event->state = WAIT_OBJECT_0;
    pthread_exit(0);
}
#endif


int connect_async(Connection *self, const wchar_t *dsn, long long timeout)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    self->env = SQL_NULL_HENV;
    self->handle = SQL_NULL_HDBC;
    self->handle_type = SQL_HANDLE_DBC;
    self->retcode = -1;
    self->event = NULL;
    self->event_status = 258;
    self->mca = 1;
    self->runned_cursors = 0;
    self->timeout = timeout;
    self->start_time = 0;
    self->rate = 1.0;
    self->state = DISCONNECTED;
    self->is_exc = 0;

    self->retcode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &self->env);
    CHECK_ERROR("connect_async::SQLAllocHandle::SQL_HANDLE_ENV");

    self->retcode = SQLSetEnvAttr(self->env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3_80, SQL_IS_INTEGER);
    CHECK_ERROR("connect_async::SQLSetEnvAttr::SQL_ATTR_ODBC_VERSION");

    self->retcode = SQLAllocHandle(SQL_HANDLE_DBC, self->env, &self->handle);
    CHECK_ERROR("connect_async::SQLAllocHandle::SQL_HANDLE_DBC");

    self->retcode = SQLSetConnectAttr(self->handle, SQL_LOGIN_TIMEOUT, (SQLPOINTER)self->timeout, SQL_IS_INTEGER);
    CHECK_ERROR("connect_async::SQLSetConnectAttr::SQL_LOGIN_TIMEOUT");

    #ifdef _WIN32
    SQLWCHAR out_conn_str[1024];
    SQLSMALLINT out_conn_str_len;

    self->retcode = SQLSetConnectAttr(
        self->handle,
        SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE,
        (SQLPOINTER)SQL_ASYNC_DBC_ENABLE_ON,
        SQL_IS_INTEGER
    );
    CHECK_ERROR("connect_async::SQLSetConnectAttr::SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE");

    self->event = CreateEvent(NULL, FALSE, FALSE, NULL);
    CHECK_EVENT_ERROR(self->event, "connect_async::CreateEvent");

    self->retcode = SQLSetConnectAttr(self->handle, SQL_ATTR_ASYNC_DBC_EVENT, self->event, SQL_IS_POINTER);
    CHECK_ERROR("connect_async::SQLSetConnectAttr::SQL_ATTR_ASYNC_DBC_EVENT");

    self->retcode = SQLDriverConnectW(
        self->handle,
        NULL,
        (SQLWCHAR *)dsn,
        SQL_NTS,
        out_conn_str,
        0,
        &out_conn_str_len,
        SQL_DRIVER_NOPROMPT
    );
    CHECK_ERROR("connect_async::SQLDriverConnectW");

    #elif __linux__
    self->event = create_t_event();
    CHECK_EVENT_ERROR(self->event, "connect_async::create_t_event");

    self->event->obj = self;
    self->event->str = dsn;

    pthread_create(&self->event->thread, NULL, t_sql_driver_connect_w, self->event);
    pthread_detach(self->event->thread);
    #endif

    return 0;
}


static PyObject* Connection_Iter(Connection *self)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    Py_INCREF(self);
    return (PyObject *)self;
}


SQLUSMALLINT get_max_concurrent_activities(SQLHDBC handle)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);
    
    SQLRETURN retcode;
    SQLUSMALLINT mca;
    retcode = SQLGetInfo(handle, SQL_MAX_CONCURRENT_ACTIVITIES, &mca, sizeof(mca), NULL);

    if (!SQL_SUCCEEDED(retcode)) {
        mca = 1;
    }

    return mca;
}


static PyObject* Connection_Next(Connection *self)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    if (self->state == CONNECTED) {
        PyErr_Format(PyExc_Exception, "(%s) The connection is already established", __FUNCTION__);
        return NULL;
    }

    if (self->state == DISCONNECTED) {
        PyErr_Format(PyExc_Exception, "(%s) The connection is already disconnected", __FUNCTION__);
        return NULL;
    }

    if (self->state == TO_CONNECT || self->state == TO_DISCONNECT) {
        if (self->event_status != WAIT_OBJECT_0) {
            if (self->start_time && \
                clock() / CLOCKS_PER_SEC - self->start_time >= self->timeout + (long long)(1 / self->rate)) {
                self->event_status = WAIT_OBJECT_0;

                PyErr_Format(PyExc_SystemError, "(%s) An event error", __FUNCTION__);
                return NULL;
            }

            #ifdef _WIN32
            self->event_status = WaitForSingleObject(self->event, (DWORD)(50 * self->rate));
            
            #elif __linux__
            self->event_status = wait_for_single_object(self->event, (unsigned long)(50 * self->rate));
            #endif

            Py_RETURN_NONE;
        }

        #ifdef _WIN32
        SQLCompleteAsync(self->handle_type, self->handle, &self->retcode);
        #endif
        PRINT_DEBUG_MESSAGE("Connection_Next::SQLCompleteAsync");

        if (self->event != NULL) {
            #ifdef _WIN32
            CloseHandle(self->event);
            PRINT_DEBUG_MESSAGE("Connection_Next::CloseHandle");

            #elif __linux__
            close_t_event(self->event);
            PRINT_DEBUG_MESSAGE("Connection_Next::close_t_event");
            #endif

            self->event = NULL;
            self->event_status = 258;
        }

        if (check_error((PyObject *)self, "Connection_Next::SQLCompleteAsync")) {
            return NULL;
        }

        if (self->state == TO_CONNECT) {
            self->mca = get_max_concurrent_activities(self->handle);
            self->state = CONNECTED;

            PRINT_DEBUG_MESSAGE("The connection is established");
        }

        if (self->state == TO_DISCONNECT) {
            self->retcode = SQLFreeHandle(SQL_HANDLE_DBC, self->handle);
            if (check_error((PyObject *)self, "Connection_Next::SQLFreeHandle::SQL_HANDLE_DBC")) {
                return NULL;
            }
            self->handle = SQL_NULL_HDBC;
            self->retcode = -1;
            self->mca = 1;
            self->runned_cursors = 0;
            self->timeout = 0;
            self->start_time = 0;
            self->rate = 1.0;
            self->state = DISCONNECTED;

            self->retcode = SQLFreeHandle(SQL_HANDLE_ENV, self->env);
            if (check_error((PyObject *)self, "Connection_Next::SQLFreeHandle::SQL_HANDLE_ENV")) {
                return NULL;
            }
            self->env = SQL_NULL_HENV;

            PRINT_DEBUG_MESSAGE("The connection is disconnected");
        }

        if (self->is_exc) {
            // raise an occured exception
            self->is_exc = 0;
            return NULL;
        }

        // return Connection object
        PyErr_SetObject(PyExc_StopIteration, (PyObject *)self);
        return NULL;
    }

    return NULL;
}


static void Connection_Dealloc(Connection *self)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    PyObject_Del(self);
}


static PyAsyncMethods Connection_Awaitable = {
    .am_await = (unaryfunc)Connection_Iter
};


int disconnect_async(Connection *self)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    if (self->state == DISCONNECTED) {
        PyErr_Format(PyExc_Exception, "(%s) The connection is already disconnected", __FUNCTION__);
        return -1;
    }

    if (self->state == CONNECTED) {
        #ifdef _WIN32
        self->event = CreateEvent(NULL, FALSE, FALSE, NULL);
        self->event_status = 258;
        CHECK_EVENT_ERROR(self->event, "disconnect_async::CreateEvent");

        self->retcode = SQLSetConnectAttr(self->handle, SQL_ATTR_ASYNC_DBC_EVENT, self->event, SQL_IS_POINTER);
        CHECK_ERROR("disconnect_async::SQLSetConnectAttr::SQL_ATTR_ASYNC_DBC_EVENT");

        self->retcode = SQLDisconnect(self->handle);
        CHECK_ERROR("disconnect_async::SQLDisconnect");

        #elif __linux__
        self->event = create_t_event();
        self->event_status = 258;
        CHECK_EVENT_ERROR(self->event, "disconnect_async::create_t_event");

        self->event->obj = self;
        pthread_create(&self->event->thread, NULL, t_sql_disconnect, self->event);
        pthread_detach(self->event->thread);
        #endif

        self->state = TO_DISCONNECT;
        if (self->timeout) {
            self->start_time = clock() / CLOCKS_PER_SEC;
        }
        return 0;
    }

    return 0;
}


static PyObject* Connection_Cursor(Connection *self)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    if (self->state != CONNECTED) {
        PyErr_Format(PyExc_Exception, "(%s) The connection isn't established", __FUNCTION__);
        return NULL;
    }

    Cursor *cursor = PyObject_New(Cursor, &Cursor_Type);
    if (cursor == NULL) {
        return NULL;
    }

    if (allocate_cursor(cursor, self) == -1) {
        Py_DECREF(cursor);
        return NULL;
    }

    cursor->state = OPENED;

    Py_INCREF(self);
    return (PyObject *)cursor;
}


static PyObject* Connection_Close(Connection *self)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    if (disconnect_async(self) == -1) {
        return NULL;
    }

    Py_INCREF(self);
    return (PyObject *)self;
}


static PyObject* Connection_Aexit(Connection *self, PyObject* args)
{
    /*
        to disconnect from the server asynchronously,
        You need to call __next__,
        for this You need to catch the exception and raise it after disconnection
    */

    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    if (Connection_Close(self) == NULL) {
        Py_INCREF(self);
    }

    PyObject *type, *value, *traceback;
    if (PyArg_ParseTuple(args, "OOO", &type, &value, &traceback) && type != Py_None) {
        // suppression an exception
        self->is_exc = 1;  // set the exception flag
    }

    return (PyObject *)self;
}


static PyMethodDef Connection_Methods[] = {
    {"__aenter__", (PyCFunction)Connection_Iter, METH_NOARGS, "Asynchronous connection"},
    {"__aexit__", (PyCFunction)Connection_Aexit, METH_VARARGS, "Asynchronous disconnection"},
    {"cursor", (PyCFunction)Connection_Cursor, METH_NOARGS, "Create a cursor"},
    {"close", (PyCFunction)Connection_Close, METH_NOARGS, "Asynchronous disconnection"},
    {NULL, NULL, 0, NULL}
};


PyTypeObject Connection_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "pyaodbc.Connection",
    .tp_doc = PyDoc_STR("Asynchronous Connection Class"),
    .tp_basicsize = sizeof(Connection),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_iter = (getiterfunc)Connection_Iter,
    .tp_iternext = (iternextfunc)Connection_Next,
    .tp_dealloc = (destructor)Connection_Dealloc,
    .tp_as_async = &Connection_Awaitable,
    .tp_methods = Connection_Methods
};
