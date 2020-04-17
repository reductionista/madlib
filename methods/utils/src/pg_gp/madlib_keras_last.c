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
//	PyObject* main = PyImport_AddModule("__main__");
//	PyObject* globalDictionary = PyModule_GetDict(main);
	PyObject* localDictionary = PyDict_New();
	PyObject *a, *b, *d;
	char *xdata = NULL;
    PyObject *py_xdata;

	elog(WARNING, "Starting function...");

    Py_ssize_t x_size = VARSIZE(pg_xdata) - VARHDRSZ;
	elog(WARNING, "Got x_size = %zd", x_size);

//	int x_ndim = ARR_NDIM(pg_xdata);
//	int *x_dims = ARR_DIMS(pg_xdata);
//	int x_items = ArrayGetNItems(x_ndim, x_dims);
//	xdata = (long *)ARR_DATA_PTR(pg_xdata);

    xdata = VARDATA(pg_xdata);
	elog(WARNING, "Got VARDATA-xdata = %ld", xdata - (char *)pg_xdata);

    if (!Py_IsInitialized()) {
        Py_Initialize();
    }

	elog(WARNING, "xdata type OID = %d", xdata_typ);
	elog(WARNING, "len(xdata) = %zd", x_size);
	elog(WARNING, "xdata[0:4] = %c%c%c%c", xdata[0], xdata[1], xdata[2], xdata[3]);

//    py_xdata = PyString_FromStringAndSize(xdata, x_size);
//    xdata[4] = '\0';
//    py_xdata = PyString_FromString(xdata);
//
    py_xdata = PyString_FromString("hello");

    char *buf = palloc(x_size+1);
    buf = PyString_AsString(py_xdata);
    elog(WARNING, "buf: %s", buf);

//    PyCodeObject* code = (PyCodeObject*) Py_CompileString("a =1 # code ", "Description?", Py_eval_input);

//	py_xlist = PyList_New(x_items);
//	for (int i=0; i < x_items; i++) {
//		elog(WARNING, "... appending %ld to py_xlist", xdata[i]);
//		PyList_SetItem(py_xlist, i, PyInt_FromLong(xdata[i]));
//	}

//	py_xlist = Py_BuildValue("[iiii]", 100,101,102,103);

//	const char* pythonScript = "result = 5 * [1,2,3]\n";
//	const char* pythonScript = "result = 5\n";
//	const char* pythonScript = "a=1\nb=2\nc=[1, 2, 3]\nd='hi'\nresult = len(d)\n";
	const char* pythonScript = "a=1";
	PyDict_SetItemString(localDictionary, "xdata", py_xdata);
	elog(WARNING, "Calling PyRun_String...");
	PyRun_String(pythonScript, Py_file_input, localDictionary, localDictionary);
	a = PyDict_GetItemString(localDictionary, "a");
	b = PyDict_GetItemString(localDictionary, "b");
	result = PyDict_GetItemString(localDictionary, "result");

//	for (Py_ssize_t i=0; i < PyList_Size(result); i++) {
//		val = PyList_GetItem(result, i);
//		elog(WARNING, "result[%ld] = %ld", i, PyInt_AsLong(val));
//	}

    if (a != NULL) {
        elog(WARNING, "After call, a=%d", PyInt_AsLong(a));
    }
    if (b != NULL) {
        elog(WARNING, "After call, b=%d", PyInt_AsLong(b));
    }
    if (d != NULL) {
        elog(WARNING, "After call, d=%s", PyString_AsString(d));
    }
	if (result == NULL) {
		elog(WARNING, "result PyObject was NULL ptr on return!");
        rv = 0;
	} else {
	    rv = PyInt_AsLong(result);
		elog(WARNING, "Return value: %ld",  rv);
        Py_DECREF(result);
	}

    Py_DECREF(py_xdata);
    Py_DECREF(localDictionary);

	Py_Finalize(); // will this mess up gpdb, if it still needs to call python later?

    retval = Int32GetDatum(rv);

    PG_RETURN_INT32(retval);
}
