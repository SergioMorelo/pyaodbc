#include "headers.h"
#include "module.h"


// begin static declarations
static PyObject* PyAODBC_Connect(PyObject *self, PyObject *args, PyObject *kwargs);
static PyModuleDef pyaodbc_module;
// end static declarations


char* get_error_message(const char *fn_name, SQLHANDLE handle, SQLSMALLINT handle_type)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    SQLRETURN retcode;
    SQLSMALLINT record_number = 1;
    SQLWCHAR sql_state[6];
    SQLINTEGER native_error;
    SQLWCHAR error_message[256];
    SQLSMALLINT error_message_len;

    wchar_t *w_sql_state;
    wchar_t *w_error_message;

    do {
        retcode = SQLGetDiagRecW(
            handle_type,
            handle,
            record_number++,
            sql_state,
            &native_error,
            error_message,
            sizeof(error_message),
            &error_message_len
        );

        if (SQL_SUCCEEDED(retcode)) {
            size_t fn_name_len = strlen(fn_name);
            char *buffer = (char *)malloc(sizeof(char) * (fn_name_len + 1000));
            if (buffer == NULL) {
                return "An allocation memory error at buffer";
            }

            #ifdef _WIN32
            w_sql_state = (wchar_t *)malloc(sizeof(sql_state));
            w_error_message = (wchar_t *)malloc(sizeof(error_message));
            if (w_sql_state == NULL && w_error_message == NULL) {
                free(buffer);
                return "An allocation memory error at wchar_t variables";
            }
            wcscpy(w_sql_state, (wchar_t *)sql_state);
            wcscpy(w_error_message, (wchar_t *)error_message);

            #elif __linux__
            w_sql_state = uctowc(sql_state);
            w_error_message = uctowc(error_message);
            if (w_sql_state == NULL && w_error_message == NULL) {
                free(buffer);
                return "An allocation memory error at wchar_t variables";
            }
            #endif
           
            int new_size = sprintf(
                buffer,
                "(%s) %ls: %hd: %ld: %ls",
                fn_name,
                w_sql_state,
                record_number,
                (long int)native_error,
                w_error_message
            );

            char *formatted_error_message = (char *)malloc(sizeof(char) * (size_t)new_size);
            if (formatted_error_message != NULL) {
                strcpy(formatted_error_message, buffer);
            }

            free(buffer);
            free(w_sql_state);
            free(w_error_message);

            return formatted_error_message; 
        }
    } while (SQL_SUCCEEDED(retcode));
    
    return NULL;
}


int check_error(PyObject *self, const char *fn_name)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    SQLRETURN retcode;
    SQLHANDLE handle;
    SQLSMALLINT handle_type;

    if (PyObject_TypeCheck(self, &Connection_Type)) {
        Connection *conn = (Connection *)self;
        retcode = conn->retcode;
        handle = conn->handle;
        handle_type = conn->handle_type;
    } else if (PyObject_TypeCheck(self, &Cursor_Type)) {
        Cursor *cursor = (Cursor *)self;
        retcode = cursor->retcode;
        handle = cursor->handle;
        handle_type = cursor->handle_type;
    } else {
        PyErr_Format(PyExc_TypeError, "(%s) An unknown type for type checking", __FUNCTION__);
        return 1;
    }

    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO && retcode != SQL_STILL_EXECUTING) {
        char *error_message = get_error_message(fn_name, handle, handle_type);
        if (error_message != NULL) {
            PyErr_Format(PyExc_Exception, "%s", error_message);
            free(error_message);
            return 1;
        } else {
            PyErr_Format(PyExc_Exception, "(%s) An unknown error in the function: %s", __FUNCTION__, fn_name);
            return 1;
        }
    }

    return 0;
}


static PyObject* PyAODBC_Connect(PyObject *self, PyObject *args, PyObject *kwargs)
{
    PRINT_DEBUG_MESSAGE(__FUNCTION__);

    static char *kwlist[] = {"dsn", "timeout", NULL};
    PyObject *py_dsn = NULL;
    long long timeout = 0;
    const wchar_t *dsn;
    Py_ssize_t string_length;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|L", kwlist, &py_dsn, &timeout)) {
        return NULL;
    }

    if (!PyUnicode_Check(py_dsn)) {
        PyErr_Format(PyExc_AttributeError, "(%s) The connection string must be an Unicode string", __FUNCTION__);
        return NULL;
    }

    string_length = PyUnicode_GET_LENGTH(py_dsn);
    dsn = PyUnicode_AsWideCharString(py_dsn, &string_length);
    if (dsn == NULL) {
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

    PyObject *module_struct = PyState_FindModule(&pyaodbc_module);
    PyObject *module_dict = PyModule_GetDict(module_struct);
    PyObject *rate = PyDict_GetItemString(module_dict, "_rate");

    if (!PyFloat_Check(rate)) {
        PyErr_Format(PyExc_ValueError, "(%s) The rate value must be float", __FUNCTION__);
        return NULL;
    }

    double rate_value = PyFloat_AsDouble(rate);
    if (rate_value < 0) {
        PyErr_Format(PyExc_AttributeError, "(%s) The rate value must be nonnegative", __FUNCTION__);
        return NULL;
    }

    Connection *conn = PyObject_GC_New(Connection, &Connection_Type);
    if (conn == NULL) {
        return NULL;
    }

    if (connect_async(conn, dsn, timeout) == -1) {
        Py_DECREF(conn);
        return NULL;
    }

    conn->rate = rate_value;
    conn->state = TO_CONNECT;
    if (timeout) {
        conn->start_time = clock() / CLOCKS_PER_SEC;
    }
    
    PyObject_GC_Track(conn);
    return (PyObject *)conn;
}


static PyMethodDef PyAODBC_Methods[] = {
    {"connect", (PyCFunction)PyAODBC_Connect, METH_VARARGS|METH_KEYWORDS, "Asynchronous connection"},
    {NULL, NULL, 0, NULL}
};


static PyModuleDef pyaodbc_module = {
    PyModuleDef_HEAD_INIT,
    .m_name = "pyaodbc",
    .m_doc = "PyAODBC Module",
    .m_size = -1,
    .m_methods = PyAODBC_Methods
};


PyMODINIT_FUNC PyInit_pyaodbc(void)
{    
    setlocale(LC_ALL, ".utf-8");  // if You need to print wchar_t symbols to the console

    if (PyType_Ready(&Connection_Type) < 0) {
        return NULL;
    }

    if (PyType_Ready(&Cursor_Type) < 0) {
        return NULL;
    }

    PyObject *module = PyModule_Create(&pyaodbc_module);
    if (module == NULL) {
        return NULL;
    }
    
    PyObject *rate = Py_BuildValue("d", 1.0);
    if (PyModule_AddObject(module, "_rate", rate) < 0) {
        goto clean_up;
    }

    Py_INCREF(&Connection_Type);
    if (PyModule_AddObject(module, "Connection", (PyObject *)&Connection_Type) < 0) {
        goto clean_up;
    }

    Py_INCREF(&Cursor_Type);
    if (PyModule_AddObject(module, "Cursor", (PyObject *)&Cursor_Type) < 0) {
        goto clean_up;
    }

    return module;

    clean_up:
        Py_XDECREF(rate);
        Py_XDECREF(&Cursor_Type);
        Py_XDECREF(&Cursor_Type);
        Py_DECREF(module);
        return NULL;
}
