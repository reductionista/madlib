#include <Python.h>
#include <postgres.h>
#include <fmgr.h>
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
    Datum   ydata           = PG_GETARG_DATUM(0);
    Oid     ydata_typ       = get_fn_expr_argtype(fcinfo->flinfo, 0);
    Datum   xdata           = PG_GETARG_DATUM(1);
    Oid     xdata_typ       = get_fn_expr_argtype(fcinfo->flinfo, 1);
    uint32_t rv = -1;
    Datum retval;

    uint32_t b = Py_IsInitialized();
    if (b) {
        elog(WARNING, "Yup, python is aleady initialized.\n");
    } else {
        elog(WARNING, "Let's go ahead an initialize this...\n");
        Py_Initialize();
    }

    elog(WARNING, "Running: import keras...");
    rv = PyRun_SimpleString("import keras");
    elog(WARNING, "Python call returned with error code %d", rv);

    retval = Int32GetDatum(rv);

    PG_RETURN_INT32(retval);
}
