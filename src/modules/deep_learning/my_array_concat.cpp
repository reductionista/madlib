#include <postgres.h>
#include <fmgr.h>
#include <utils/array.h>
#include <utils/memutils.h>
#include <catalog/pg_type.h>
//#include <dbconnector/Backend.hpp>
#include "my_array_concat.hpp"

namespace madlib {

namespace modules {

namespace deep_learning {

/* useful macros for accessing float4 arrays */
#define ARRNELEMS(x)  ArrayGetNItems(ARR_NDIM(x), ARR_DIMS(x))

/* Create a new "ndims"-D array of floats with room for "num" elements */
ArrayType *
new_floatArrayType(int ndims, int *shape, int num)
{
	ArrayType  *r;
	unsigned long nbytes = ARR_OVERHEAD_NONULLS(1) + sizeof(float) * num;

	r = (ArrayType *) palloc0(nbytes);

	SET_VARSIZE(r, nbytes);
	ARR_NDIM(r) = ndims;
	r->dataoffset = 0;			/* marker for no null bitmap */
    for (int i; i < ndims; i++) {
        ARR_DIMS(r)[i] = shape[i];
    }

	ARR_ELEMTYPE(r) = FLOAT4OID;
	ARR_LBOUND(r)[0] = 1;

	return r;
}

ArrayType *
copy_floatArrayType(ArrayType *a)
{
	ArrayType  *r;
	int	n = ARRNELEMS(a);

	r = new_floatArrayType(1, ARR_DIMS(a), n);
	memcpy(ARR_DATA_PTR(r), ARR_DATA_PTR(a), n * sizeof(int32));
	return r;
}

// ----------------------------------------------------------------------

Datum my_array_concat_transition(PG_FUNCTION_ARGS)
{
    // args 0 = state
    // args 1 = array of doubles
    //
    ArrayType  *a, *b;
    float *pa;
    float *pb;
    int num_current_elems, num_new_elems;

    const int buffer_size = 700*1024*1024;

    b = PG_GETARG_ARRAYTYPE_P(1);

//    if (AggCheckCallContext(fcinfo, NULL)) {
    if (1) {
        if (PG_ARGISNULL(0)) {
            PG_RETURN_POINTER(copy_floatArrayType(b));
            // Need to resize to be able to hold all images in 1 buffer
            // b.resize(buffer_size)
        } else {
            a = PG_GETARG_ARRAYTYPE_P(0);
        }
    } else {
        if (PG_ARGISNULL(0)) {
            ereport(ERROR, (errmsg("First operand must be non-null")));
        }
        a = PG_GETARG_ARRAYTYPE_P_COPY(0);
    }

    if (ARR_NDIM(a) != ARR_NDIM(b)) {
        ereport(ERROR, (errmsg("All arrays must have the same number of dimensions")));
    }

    num_current_elems = ArrayGetNItems(ARR_NDIM(a), ARR_DIMS(a));
    num_new_elems = ArrayGetNItems(ARR_NDIM(b), ARR_DIMS(b));

    if (num_current_elems + num_new_elems > buffer_size) {
        ereport(ERROR, (errmsg("Buffer overflow!")));
    }

    pa = (float *) ARR_DATA_PTR(a);
    pb = (float *) ARR_DATA_PTR(b);

    memcpy(pa + num_current_elems, pb, num_new_elems * sizeof(float));

    PG_RETURN_POINTER(a);
}

} // deep_learning

} // modules

} // madlib
