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
 * @param ydata
 * @param xdata
 *
 */
Datum
dlc_madlib_keras_fit(PG_FUNCTION_ARGS) {
    ArrayType *pg_ydata           = PG_GETARG_ARRAYTYPE_P(0);
    Oid     ydata_typ          = get_fn_expr_argtype(fcinfo->flinfo, 0);
    ArrayType *pg_xdata           = PG_GETARG_ARRAYTYPE_P(1);
    Oid     xdata_typ          = get_fn_expr_argtype(fcinfo->flinfo, 1);
	PyObject *return_val;
    uint32_t rv = -1;
    Datum retval;
	PyObject *result;
//	PyObject* main = PyImport_AddModule("__main__");
//	PyObject* globalDictionary = PyModule_GetDict(main);
	PyObject* localDictionary = PyDict_New();
	PyObject* val;
	ArrayType *a;
	PyObject *py_xlist;
	long *c_xdata = NULL;

	int x_ndim = ARR_NDIM(pg_xdata);
	int *x_dims = ARR_DIMS(pg_xdata);
	int x_items = ArrayGetNItems(x_ndim, x_dims);
	c_xdata = (long *)ARR_DATA_PTR(pg_xdata);

//	elog(WARNING, "xdata_type = %d", xdata_typ);

    if (!Py_IsInitialized()) {
        Py_Initialize();
    }

	py_xlist = PyList_New(x_items);
	for (int i=0; i < x_items; i++) {
//		elog(WARNING, "... appending %ld to py_xlist", c_xdata[i]);
		PyList_SetItem(py_xlist, i, PyInt_FromLong(c_xdata[i]));
	}

//	py_xlist = Py_BuildValue("[iiii]", 100,101,102,103);

	const char* pythonScript = "result = [ 5*x for x in xdata ]\n";
//	const char* pythonScript = "result = 5 * [1,2,3]\n";
	PyDict_SetItemString(localDictionary, "xdata", py_xlist);
	PyRun_String(pythonScript, Py_file_input, localDictionary, localDictionary);
	result = PyDict_GetItemString(localDictionary, "result");

//	for (Py_ssize_t i=0; i < PyList_Size(result); i++) {
//		val = PyList_GetItem(result, i);
//		elog(WARNING, "result[%ld] = %ld", i, PyInt_AsLong(val));
//	}
	
	if (result == NULL) {
		elog(ERROR, "result PyObject was NULL ptr on return!");
        rv = 0;
	} else {
	    rv = PyList_Size(result);
//		elog(WARNING, "Return value: %ld",  rv);
	}
	Py_Finalize();

    retval = Int32GetDatum(rv);

    PG_RETURN_INT32(retval);
}
