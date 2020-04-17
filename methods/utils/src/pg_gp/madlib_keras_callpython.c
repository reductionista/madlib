#include <Python.h>
#include <postgres.h>
#include <fmgr.h>
#include <utils/array.h>
#include <utils/builtins.h>

#ifndef NO_PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(dlc_madlib_keras_fit);
/*
 * @brief Call keras.fit() from a C UDF
 *
 * @param xdata
 *
 */
Datum
dlc_madlib_keras_fit(PG_FUNCTION_ARGS) {
    bytea *pg_xdata           = PG_GETARG_BYTEA_P(0);
    Oid     xdata_typ          = get_fn_expr_argtype(fcinfo->flinfo, 0);
    uint32_t rv = -1;
    Datum retval;
	PyObject *result;
	PyObject* localDictionary = PyDict_New();
	char *xdata = NULL;
    PyObject *py_xdata;
    int length=0;
   
    Py_ssize_t x_size = VARSIZE(pg_xdata) - VARHDRSZ;

    xdata = VARDATA(pg_xdata);

    if (!Py_IsInitialized()) {
        Py_Initialize();
    }

    py_xdata = PyString_FromStringAndSize(xdata,  x_size);

	const char* pythonScript = "result = xdata[:50]";

	PyDict_SetItemString(localDictionary, "xdata", py_xdata);
    result = PySequence_GetSlice(py_xdata, 0, 50);
//	PyExec_Code(pythonCode, Py_file_input, localDictionary, localDictionary);
//	PyRun_String(pythonScript, Py_file_input, localDictionary, localDictionary);

    if (PyErr_Occurred()) {
        PyErr_Print();
	    Py_Finalize(); // will this mess up gpdb, if it still needs to call python later?
        retval = Int32GetDatum(-1);
        PG_RETURN_INT32(retval);
    }

//	result = PyDict_GetItemString(localDictionary, "result");

	if (result == NULL) {
		elog(WARNING, "result PyObject was NULL ptr on return!");
	} else {
        length = PyObject_Length(result);
        Py_DECREF(result);
	}

    Py_DECREF(py_xdata);
    Py_DECREF(localDictionary);

//	Py_Finalize(); // will this mess up gpdb, if it still needs to call python later?

    retval = Int32GetDatum(length);

    PG_RETURN_INT32(retval);
}
