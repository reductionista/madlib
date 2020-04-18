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
    bytea *pg_xdata           = PG_GETARG_BYTEA_P(1);
    Oid     xdata_typ          = get_fn_expr_argtype(fcinfo->flinfo, 1);
    uint32_t rv = -1;
    Datum retval;
	char *xdata = NULL;
    int length=0;
    char *result;
   
    int x_size = VARSIZE(pg_xdata) - VARHDRSZ;

    xdata = VARDATA(pg_xdata);

//    result = palloc(x_size + 2);
//    memcpy(result, xdata, x_size);
//    result[x_size - 2] = "h";
//    result[x_size - 1] = "i";
    result = xdata;

	if (result == NULL) {
		elog(WARNING, "result PyObject was NULL ptr on return!");
	} else {
        length = x_size;
	}

    retval = Int32GetDatum(length);

//    pfree(result);
    PG_RETURN_INT32(retval);
}
