#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq/pqformat.h"
#include "parser/parse_coerce.h"
#include "catalog/pg_type.h"
#include "utils/array.h"
#include "utils/numeric.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/int8.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "access/hash.h"
#include <math.h>

#ifndef NO_PG_MODULE_MAGIC
    PG_MODULE_MAGIC;
#endif

/*
This module provides c implementations for several postgres array operations.

There is a 3 tier structure to each function calls. This is done to take
advantage of some common type checking. All the functions are classified into
4 types based on the input/output types:
function(array,array)->array (calls: General_2Array_to_Array)
function(array,scalar)->array (calls: General_Array_to_Array)
function(array,array)->scalar (calls: General_2Array_to_Element)
function(array,scalar)->scalar (calls: General_Array_to_Element)
function(array,scalar)->struct (calls: General_Array_to_Struct)

Assuming that this input is flexible enough for some new function, implementer
needs to provide 2 functions. First is the top level function that is being
exposed to SQL and takes the necessary parameters. This function makes a call
to one of the 4 intermediate functions, passing pointer to the low level
function (or functions) as the argument. In case of the single function, this
low level function, it will specify the operations to be executed against each
cell in the array. If two functions are to be passed the second is the
finalization function that takes the result of the execution on each cell and
produces final result. In case not final functions is necessary a generic
'noop_finalize' can be used - which does nothing to the intermediate result.
*/

static ArrayType *General_2Array_to_Array(ArrayType *v1, ArrayType *v2,
        Datum(*element_function)(Datum,Oid,Datum,Oid,Datum,Oid));
static ArrayType *General_Array_to_Array(ArrayType *v1, Datum value,
        Datum(*element_function)(Datum,Oid,Datum,Oid,Datum,Oid));
static ArrayType *General_Array_to_Cumulative_Array(ArrayType *v1, Datum value,
        Datum(*element_function)(Datum,Oid,Datum,Oid,Datum,Oid));
static Datum General_2Array_to_Element(ArrayType *v1, ArrayType *v2,
        Datum(*element_function)(Datum,Oid,Datum,Oid,Datum,Oid),
        Datum(*finalize_function)(Datum,int,Oid));
static Datum General_Array_to_Element(ArrayType *v, Datum exta_val, float8 init_val,
        Datum(*element_function)(Datum,Oid,Datum,Oid,Datum,Oid),
        Datum(*finalize_function)(Datum,int,Oid));
static Datum General_Array_to_Struct(ArrayType *v, void *init_val,
        void*(*element_function)(Datum,Oid,int,void*),
        Datum(*finalize_function)(void*,int,Oid));

static inline Datum noop_finalize(Datum elt,int size,Oid element_type);
static inline Datum average_finalize(Datum elt,int size,Oid element_type);
static inline Datum average_root_finalize(Datum elt,int size,Oid element_type);
static inline Datum value_index_finalize(void *mid_result,int size,Oid element_type);

static inline Datum element_cos(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type);
static inline Datum element_add(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type);
static inline Datum element_sub(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type);
static inline Datum element_mult(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type);
static inline Datum element_div(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type);
static inline Datum element_set(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type);
static inline Datum element_abs(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type);
static inline Datum element_sqrt(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type);
static inline Datum element_pow(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type);
static inline Datum element_square(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type);
static inline Datum element_dot(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type);
static inline Datum element_contains(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type);
static inline Datum element_max(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type);
static inline Datum element_min(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type);
static inline void* element_argmax(Datum element, Oid elt_type, int elt_index, void *result);
static inline void* element_argmin(Datum element, Oid elt_type, int elt_index, void *result);
static inline Datum element_sum(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type);
static inline Datum element_abs_sum(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type);
static inline Datum element_diff(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type);
static inline Datum element_sum_sqr(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type);

static ArrayType* array_to_float8_array(ArrayType *a);

static inline int64 datum_int64_cast(Datum elt, Oid element_type);
static inline Datum int64_datum_cast(int64 result, Oid result_type);
static inline float8 datum_float8_cast(Datum elt, Oid element_type);
static inline Datum float8_datum_cast(float8 result, Oid result_type);

static inline Datum element_op(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type,
        float8 (*op)(float8, float8, float8));

static inline float8 float8_cos(float8 op1, float8 op2, float8 opt_op);
static inline float8 float8_add(float8 op1, float8 op2, float8 opt_op);
static inline float8 float8_sub(float8 op1, float8 op2, float8 opt_op);
static inline float8 float8_mult(float8 op1, float8 op2, float8 opt_op);
static inline float8 float8_div(float8 op1, float8 op2, float8 opt_op);
static inline float8 float8_set(float8 op1, float8 op2, float8 opt_op);
static inline float8 float8_abs(float8 op1, float8 op2, float8 opt_op);
static inline float8 float8_sqrt(float8 op1, float8 op2, float8 opt_op);
static inline float8 float8_pow(float8 op1, float8 op2, float8 opt_op);
static inline float8 float8_square(float8 op1, float8 op2, float8 opt_op);
static inline float8 float8_dot(float8 op1, float8 op2, float8 opt_op);
static inline float8 float8_contains(float8 op1, float8 op2, float8 opt_op);
static inline float8 float8_max(float8 op1, float8 op2, float8 opt_op);
static inline float8 float8_min(float8 op1, float8 op2, float8 opt_op);
static inline float8 float8_sum(float8 op1, float8 op2, float8 opt_op);
static inline float8 float8_abs_sum(float8 op1, float8 op2, float8 opt_op);
static inline float8 float8_diff(float8 op1, float8 op2, float8 opt_op);
static inline float8 float8_sum_sqr(float8 op1, float8 op2, float8 opt_op);

/*
 * Implementation of operations on float8 type
 */
static
inline
float8
float8_cos(float8 op1, float8 op2, float8 opt_op){
    (void) op2;
    (void) opt_op;
    return cos(op1);
}

static
inline
float8
float8_add(float8 op1, float8 op2, float8 opt_op){
    (void) op2;
    return op1 + opt_op;
}

static
inline
float8
float8_sub(float8 op1, float8 op2, float8 opt_op){
    (void) op2;
    return op1 - opt_op;
}

static
inline
float8
float8_mult(float8 op1, float8 op2, float8 opt_op){
    (void) op2;
    return op1 * opt_op;
}

static
inline
int64
int64_div(int64 num, int64 denom){
    if (denom == 0) {
        ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO),
            errmsg("division by zero is not allowed"),
            errdetail("Arrays with element 0 can not be use in the denominator")));
    }
    return num / denom;
}

static
inline
float8
float8_div(float8 op1, float8 op2, float8 opt_op){
    (void) op2;
    if (opt_op == 0) {
        ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO),
            errmsg("division by zero is not allowed"),
            errdetail("Arrays with element 0 can not be use in the denominator")));
    }
    return op1 / opt_op;
}

static
inline
float8
float8_set(float8 op1, float8 op2, float8 opt_op){
    (void) op2;
    (void) op1;
    return opt_op;
}

static
inline
float8
float8_abs(float8 op1, float8 op2, float8 opt_op){
    (void) op2;
    (void) opt_op;
    return fabs(op1);
}

static
inline
float8
float8_sqrt(float8 op1, float8 op2, float8 opt_op){
    (void) op2;
    (void) opt_op;
    if (op1 < 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("square root of negative values is not allowed"),
            errdetail("Arrays with negative values can not be input of array_sqrt")));
    }
    return sqrt(op1);
}

static
inline
float8
float8_pow(float8 op1, float8 op2, float8 opt_op){
	(void) op2;
	return pow(op1, opt_op);
}

static
inline
float8
float8_square(float8 op1, float8 op2, float8 opt_op){
    (void) op2;
    (void) opt_op;
    return op1*op1;
}

static
inline
float8
float8_dot(float8 op1, float8 op2, float8 opt_op){
    return op2 + op1 * opt_op;
}

static
inline
float8
float8_contains(float8 op1, float8 op2, float8 opt_op){
    return op2 + (((op1 == opt_op) || (opt_op == 0))? 0 : 1);
}

static
inline
float8
float8_max(float8 op1, float8 op2, float8 opt_op) {
    (void) opt_op;
    return (op1 > op2) ? op1:op2;
}

static
inline
float8
float8_min(float8 op1, float8 op2, float8 opt_op) {
     (void) opt_op;
     return (op1 < op2) ? op1: op2;
}

static
inline
float8
float8_sum(float8 op1, float8 op2, float8 opt_op){
    (void) opt_op;
    return op1 + op2;
}


static
inline
float8
float8_abs_sum(float8 op1, float8 op2, float8 opt_op){
    (void) opt_op;
    return fabs(op1) + op2;
}

static
inline
float8
float8_diff(float8 op1, float8 op2, float8 opt_op){
    return op2 + (op1 - opt_op) * (op1 - opt_op);
}

static
inline
float8
float8_sum_sqr(float8 op1, float8 op2, float8 opt_op){
    (void) opt_op;
    return op2 + op1 * op1;
}
/*
 * Assume the input array is of type numeric[].
 */
ArrayType*
array_to_float8_array(ArrayType *x) {
    Oid element_type = ARR_ELEMTYPE(x);
    if (element_type == FLOAT8OID) {
        // this does not returning a copy, the caller needs to pay attention
        return x;
    }

    // deconstruct
    TypeCacheEntry * TI = lookup_type_cache(element_type,
                                            TYPECACHE_CMP_PROC_FINFO);
    bool *nulls = NULL;
    int len = 0;
    Datum *array = NULL;
    deconstruct_array(x,
                      element_type,
                      TI->typlen,
                      TI->typbyval,
                      TI->typalign,
                      &array,
                      &nulls,
                      &len);

    // casting
    Datum *float8_array = (Datum *)palloc(len * sizeof(Datum));
    int i;
    for (i = 0; i < len; i ++) {
        if (nulls[i]) { float8_array[i] = Float8GetDatum(0); }
        else {
            float8_array[i] = Float8GetDatum(
                    datum_float8_cast(array[i], element_type));
        }
    }

    // reconstruct
    TypeCacheEntry * FLOAT8TI = lookup_type_cache(FLOAT8OID,
                                                  TYPECACHE_CMP_PROC_FINFO);
    ArrayType *ret = construct_md_array(float8_array,
                                        nulls,
                                        ARR_NDIM(x),
                                        ARR_DIMS(x),
                                        ARR_LBOUND(x),
                                        FLOAT8OID,
                                        FLOAT8TI->typlen,
                                        FLOAT8TI->typbyval,
                                        FLOAT8TI->typalign);

    pfree(array);
    pfree(float8_array);
    pfree(nulls);

    return ret;
}

// ------------------------------------------------------------------------
// exported functions
// ------------------------------------------------------------------------
/*
 * This function returns an float array with specified size.
 */
PG_FUNCTION_INFO_V1(array_of_float);
Datum
array_of_float(PG_FUNCTION_ARGS){
    int size = PG_GETARG_INT32(0);
    if (size <= 0 || size > 10000000) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("invalid array length"),
                 errdetail("array_of_float: Size should be in [1, 1e7], %d given", size)));
    }
    Datum* array = palloc (sizeof(Datum)*size);
    for(int i = 0; i < size; ++i) {
        array[i] = Float8GetDatum(0);
    }

    TypeCacheEntry *typentry = lookup_type_cache(FLOAT8OID,TYPECACHE_CMP_PROC_FINFO);
    int type_size = typentry->typlen;
    bool typbyval = typentry->typbyval;
    char typalign = typentry->typalign;

    ArrayType *pgarray = construct_array(array,size,FLOAT8OID,type_size,typbyval,typalign);
    PG_RETURN_ARRAYTYPE_P(pgarray);
}

/*
 * This function returns an integer array with specified size.
 */
PG_FUNCTION_INFO_V1(array_of_bigint);
Datum
array_of_bigint(PG_FUNCTION_ARGS){
    int size = PG_GETARG_INT32(0);
    if (size <= 0 || size > 10000000) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("invalid array length"),
                 errdetail("array_of_bigint: Size should be in [1, 1e7], %d given", size)));
    }
    Datum* array = palloc (sizeof(Datum)*size);
    for(int i = 0; i < size; ++i) {
        array[i] = Int64GetDatum(0);
    }

    TypeCacheEntry *typentry = lookup_type_cache(INT8OID,TYPECACHE_CMP_PROC_FINFO);
    int type_size = typentry->typlen;
    bool typbyval = typentry->typbyval;
    char typalign = typentry->typalign;

    ArrayType *pgarray = construct_array(array,size,INT8OID,type_size,typbyval,typalign);
    PG_RETURN_ARRAYTYPE_P(pgarray);
}

/*
 * This function returns standard deviation of the array elements.
 */
PG_FUNCTION_INFO_V1(array_stddev);
Datum
array_stddev(PG_FUNCTION_ARGS){
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }

    ArrayType *x = PG_GETARG_ARRAYTYPE_P(0);

    Datum mean = General_Array_to_Element(x, Float8GetDatum(0), 0.0,
            element_sum, average_finalize);
    Datum res = General_Array_to_Element(x, mean, 0.0,
            element_diff, average_root_finalize);

    PG_FREE_IF_COPY(x, 0);

    return(res);
}

/*
 * This function returns mean of the array elements.
 */
PG_FUNCTION_INFO_V1(array_mean);
Datum
array_mean(PG_FUNCTION_ARGS){
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }

    ArrayType *v = PG_GETARG_ARRAYTYPE_P(0);

    Datum res = General_Array_to_Element(v, Float8GetDatum(0), 0.0,
            element_sum, average_finalize);

    PG_FREE_IF_COPY(v, 0);

    return(res);
}

/*
 * This function returns sum of the array elements, typed float8.
 */
PG_FUNCTION_INFO_V1(array_sum_big);
Datum
array_sum_big(PG_FUNCTION_ARGS){
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }

    ArrayType *v = PG_GETARG_ARRAYTYPE_P(0);

    Datum res = General_Array_to_Element(v, Float8GetDatum(0), 0.0,
            element_sum, noop_finalize);

    PG_FREE_IF_COPY(v, 0);

    return(res);
}

/*
 * This function returns sum of the array elements.
 */
PG_FUNCTION_INFO_V1(array_sum);
Datum
array_sum(PG_FUNCTION_ARGS){
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }

    ArrayType *v = PG_GETARG_ARRAYTYPE_P(0);
    Oid element_type = ARR_ELEMTYPE(v);

    Datum res = General_Array_to_Element(v, Float8GetDatum(0), 0.0,
            element_sum, noop_finalize);

    PG_FREE_IF_COPY(v, 0);

    return float8_datum_cast(DatumGetFloat8(res), element_type);
}


/*
 * This function returns sum of the array elements' absolute value.
 */
PG_FUNCTION_INFO_V1(array_abs_sum);
Datum
array_abs_sum(PG_FUNCTION_ARGS){
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }

    ArrayType *v = PG_GETARG_ARRAYTYPE_P(0);
    Oid element_type = ARR_ELEMTYPE(v);

    Datum res = General_Array_to_Element(v, Float8GetDatum(0), 0.0,
            element_abs_sum, noop_finalize);

    PG_FREE_IF_COPY(v, 0);

    return float8_datum_cast(DatumGetFloat8(res), element_type);
}


/*
 * This function returns minimum of the array elements.
 */
PG_FUNCTION_INFO_V1(array_min);
Datum
array_min(PG_FUNCTION_ARGS){
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }

    ArrayType *v = PG_GETARG_ARRAYTYPE_P(0);
    Oid element_type = ARR_ELEMTYPE(v);
    Datum res = General_Array_to_Element(v, Float8GetDatum(0), get_float8_infinity(),
            element_min, noop_finalize);

    PG_FREE_IF_COPY(v, 0);

    return float8_datum_cast(DatumGetFloat8(res), element_type);
}

/*
 * This function returns maximum of the array elements.
 */
PG_FUNCTION_INFO_V1(array_max);
Datum
array_max(PG_FUNCTION_ARGS){
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }

    ArrayType *v = PG_GETARG_ARRAYTYPE_P(0);
    Oid element_type = ARR_ELEMTYPE(v);

    Datum res = General_Array_to_Element(v, Float8GetDatum(0), -get_float8_infinity(),
            element_max, noop_finalize);

    PG_FREE_IF_COPY(v, 0);

    return float8_datum_cast(DatumGetFloat8(res), element_type);
}

typedef struct
{
    float8 value;
    int64  index;
} value_index;

/*
 * This function returns maximum and corresponding index of the array elements.
 */
PG_FUNCTION_INFO_V1(array_max_index);
Datum
array_max_index(PG_FUNCTION_ARGS) {
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }
    ArrayType *v = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(v) != 1) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Input array with multiple dimensions is not allowed!")));
    }

    value_index *result = (value_index *)palloc(sizeof(value_index));
    result->value = -get_float8_infinity();
    result->index = 0;

    Datum res = General_Array_to_Struct(v, result, element_argmax, value_index_finalize);

    pfree(result);
    PG_FREE_IF_COPY(v, 0);
    return res;
}

/*
 * This function returns minimum and corresponding index of the array elements.
 */
PG_FUNCTION_INFO_V1(array_min_index);
Datum
array_min_index(PG_FUNCTION_ARGS) {
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }
    ArrayType *v = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(v) != 1) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Input array with multiple dimensions is not allowed!")));
    }

    value_index *result = (value_index *)palloc(sizeof(value_index));
    result->value = get_float8_infinity();
    result->index = 0;

    Datum res = General_Array_to_Struct(v, result, element_argmin, value_index_finalize);

    PG_FREE_IF_COPY(v, 0);

    pfree(result);
    return res;
}

/*
 * This function returns dot product of two arrays as vectors.
 */
PG_FUNCTION_INFO_V1(array_dot);
Datum
array_dot(PG_FUNCTION_ARGS){
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }
    if (PG_ARGISNULL(1)) { PG_RETURN_NULL(); }

    ArrayType *v1 = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *v2 = PG_GETARG_ARRAYTYPE_P(1);

    Datum res = General_2Array_to_Element(v1, v2, element_dot, noop_finalize);

    PG_FREE_IF_COPY(v1, 0);
    PG_FREE_IF_COPY(v2, 1);

    return(res);
}

/*
 * This function checks if each non-zero element in the right array equals to
 * the element with the same index in the left array.
 */
PG_FUNCTION_INFO_V1(array_contains);
Datum
array_contains(PG_FUNCTION_ARGS){
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }
    if (PG_ARGISNULL(1)) { PG_RETURN_NULL(); }

    ArrayType *v1 = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *v2 = PG_GETARG_ARRAYTYPE_P(1);

    Datum res = General_2Array_to_Element(v1, v2, element_contains, noop_finalize);

    PG_FREE_IF_COPY(v1, 0);
    PG_FREE_IF_COPY(v2, 1);

    if (DatumGetFloat8(res) == 0.) {
        PG_RETURN_BOOL(true);
    } else {
        PG_RETURN_BOOL(false);
    }
}

/*
 * This function returns the element-wise sum of two arrays.
 */
PG_FUNCTION_INFO_V1(array_add);
Datum
array_add(PG_FUNCTION_ARGS){
    // special handling for madlib.sum()
    if (PG_ARGISNULL(0) && PG_ARGISNULL(1)) { PG_RETURN_NULL(); }
    if (PG_ARGISNULL(0)) {
        PG_RETURN_ARRAYTYPE_P(PG_GETARG_ARRAYTYPE_P(1));
    }
    if (PG_ARGISNULL(1)) {
        PG_RETURN_ARRAYTYPE_P(PG_GETARG_ARRAYTYPE_P(0));
    }

    ArrayType *v1 = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *v2 = PG_GETARG_ARRAYTYPE_P(1);

    ArrayType *res = General_2Array_to_Array(v1, v2, element_add);

    PG_FREE_IF_COPY(v1, 0);
    PG_FREE_IF_COPY(v2, 1);

    PG_RETURN_ARRAYTYPE_P(res);
}

/*
 * This function returns the element-wise difference of two arrays.
 */
PG_FUNCTION_INFO_V1(array_sub);
Datum
array_sub(PG_FUNCTION_ARGS){
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }
    if (PG_ARGISNULL(1)) { PG_RETURN_NULL(); }

    ArrayType *v1 = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *v2 = PG_GETARG_ARRAYTYPE_P(1);

    ArrayType *res = General_2Array_to_Array(v1, v2, element_sub);

    PG_FREE_IF_COPY(v1, 0);
    PG_FREE_IF_COPY(v2, 1);

    PG_RETURN_ARRAYTYPE_P(res);
}

/*
 * This function returns the element-wise product of two arrays.
 */
PG_FUNCTION_INFO_V1(array_mult);
Datum
array_mult(PG_FUNCTION_ARGS){
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }
    if (PG_ARGISNULL(1)) { PG_RETURN_NULL(); }

    ArrayType *v1 = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *v2 = PG_GETARG_ARRAYTYPE_P(1);

    ArrayType *res = General_2Array_to_Array(v1, v2, element_mult);

    PG_FREE_IF_COPY(v1, 0);
    PG_FREE_IF_COPY(v2, 1);

    PG_RETURN_ARRAYTYPE_P(res);
}

/*
 * This function returns result of element-wise division of two arrays.
 */
PG_FUNCTION_INFO_V1(array_div);
Datum
array_div(PG_FUNCTION_ARGS){
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }
    if (PG_ARGISNULL(1)) { PG_RETURN_NULL(); }

    ArrayType *v1 = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *v2 = PG_GETARG_ARRAYTYPE_P(1);

    ArrayType *res = General_2Array_to_Array(v1, v2, element_div);

    PG_FREE_IF_COPY(v1, 0);
    PG_FREE_IF_COPY(v2, 1);

    PG_RETURN_ARRAYTYPE_P(res);
}

/*
 * This function takes the absolute value for each element.
 */
PG_FUNCTION_INFO_V1(array_abs);
Datum
array_abs(PG_FUNCTION_ARGS){
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }

    ArrayType *v1 = PG_GETARG_ARRAYTYPE_P(0);

    Oid element_type = ARR_ELEMTYPE(v1);
    Datum v2 = float8_datum_cast(0, element_type);

    ArrayType *res = General_Array_to_Array(v1, v2, element_abs);

    PG_FREE_IF_COPY(v1, 0);

    PG_RETURN_ARRAYTYPE_P(res);
}


/*
 * This function takes the square root for each element.
 */
PG_FUNCTION_INFO_V1(array_sqrt);
Datum
array_sqrt(PG_FUNCTION_ARGS){
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }

    ArrayType *x = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *v1 = array_to_float8_array(x);
    Datum v2 = 0;

    ArrayType *res = General_Array_to_Array(v1, v2, element_sqrt);

    if (v1 != x) { pfree(v1); }
    PG_FREE_IF_COPY(x, 0);

    PG_RETURN_ARRAYTYPE_P(res);
}

/*
 * This function takes the power for each element.
 */
PG_FUNCTION_INFO_V1(array_pow);
Datum
array_pow(PG_FUNCTION_ARGS){
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }
    if (PG_ARGISNULL(1)) { PG_RETURN_NULL(); }

    ArrayType *v1 = PG_GETARG_ARRAYTYPE_P(0);
    Datum v2 = PG_GETARG_DATUM(1);

    ArrayType *res = General_Array_to_Array(v1, v2, element_pow);

    PG_FREE_IF_COPY(v1, 0);

    PG_RETURN_ARRAYTYPE_P(res);
}

/*
 * This function takes the square for each element.
 */
PG_FUNCTION_INFO_V1(array_square);
Datum
array_square(PG_FUNCTION_ARGS){
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }

    ArrayType *x = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *v1 = array_to_float8_array(x);
    Datum v2 = 0;

    ArrayType *res = General_Array_to_Array(v1, v2, element_square);

    if (v1 != x) { pfree(v1); }
    PG_FREE_IF_COPY(x, 0);

    PG_RETURN_ARRAYTYPE_P(res);
}

/*
 * This function sets all elements to be the specified value.
 */
PG_FUNCTION_INFO_V1(array_fill);
Datum
array_fill(PG_FUNCTION_ARGS){
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }
    if (PG_ARGISNULL(1)) { PG_RETURN_NULL(); }

    ArrayType *v1 = PG_GETARG_ARRAYTYPE_P(0);
    Datum v2 = PG_GETARG_DATUM(1);

    ArrayType *res = General_Array_to_Array(v1, v2, element_set);

    PG_FREE_IF_COPY(v1, 0);

    PG_RETURN_ARRAYTYPE_P(res);
}

/*
 * This function apply cos function to each element.
 */
PG_FUNCTION_INFO_V1(array_cos);
Datum
array_cos(PG_FUNCTION_ARGS){
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }

    ArrayType *v1 = PG_GETARG_ARRAYTYPE_P(0);
    Oid element_type = ARR_ELEMTYPE(v1);
    Datum v2 = float8_datum_cast(0, element_type);

    ArrayType *res = General_Array_to_Array(v1, v2, element_cos);

    PG_FREE_IF_COPY(v1, 0);

    PG_RETURN_ARRAYTYPE_P(res);
}

/*
 * This function multiplies the specified value to each element.
 */
PG_FUNCTION_INFO_V1(array_scalar_mult);
Datum
array_scalar_mult(PG_FUNCTION_ARGS){
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }
    if (PG_ARGISNULL(1)) { PG_RETURN_NULL(); }

    ArrayType *v1 = PG_GETARG_ARRAYTYPE_P(0);
    Datum v2 = PG_GETARG_DATUM(1);

    ArrayType *res = General_Array_to_Array(v1, v2, element_mult);

    PG_FREE_IF_COPY(v1, 0);

    PG_RETURN_ARRAYTYPE_P(res);
}

/*
 * This function addes the specified value to each element.
 */
PG_FUNCTION_INFO_V1(array_scalar_add);
Datum
array_scalar_add(PG_FUNCTION_ARGS){
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }
    if (PG_ARGISNULL(1)) { PG_RETURN_NULL(); }

    ArrayType *v1 = PG_GETARG_ARRAYTYPE_P(0);
    Datum v2 = PG_GETARG_DATUM(1);

    ArrayType *res = General_Array_to_Array(v1, v2, element_add);

    PG_FREE_IF_COPY(v1, 0);

    PG_RETURN_ARRAYTYPE_P(res);
}

/*
 * This function returns the cumulative sum of the array elements.
 */
PG_FUNCTION_INFO_V1(array_cum_sum);
Datum
array_cum_sum(PG_FUNCTION_ARGS){
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }

    ArrayType *v = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *res = General_Array_to_Cumulative_Array(v, Float8GetDatum(0.0), element_add);

    PG_FREE_IF_COPY(v, 0);
    PG_RETURN_ARRAYTYPE_P(res);
}

/*
 * This function returns the cumulative product of the array elements.
 */
PG_FUNCTION_INFO_V1(array_cum_prod);
Datum
array_cum_prod(PG_FUNCTION_ARGS){
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }

    ArrayType *v = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *res = General_Array_to_Cumulative_Array(v, Float8GetDatum(1.0), element_mult);

    PG_FREE_IF_COPY(v, 0);
    PG_RETURN_ARRAYTYPE_P(res);
}

/*
 * This function removes elements with specified value from an array.
 */
PG_FUNCTION_INFO_V1(array_filter);
Datum
array_filter(PG_FUNCTION_ARGS) {
    ArrayType * arr = PG_GETARG_ARRAYTYPE_P(0);
    if (ARR_NDIM(arr) != 1) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Input array with multiple dimensions is not allowed!")));
    }

    if (ARR_HASNULL(arr)) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("Input array with nulls is not allowed!")));
    }

    // value to be filtered
    Oid element_type = ARR_ELEMTYPE(arr);
    Datum val = float8_datum_cast(0., element_type);
    char op[3] = "!=";
    if (PG_NARGS() > 1) { val = PG_GETARG_DATUM(1); }
    if (PG_NARGS() > 2) {
        text *op_text = PG_GETARG_TEXT_P(2);
        int op_len = VARSIZE(op_text) - VARHDRSZ;
        strncpy(op, VARDATA(op_text), VARSIZE(op_text) - VARHDRSZ);
        op[op_len] = 0;
    }

    // deconstruct
    TypeCacheEntry * TI = lookup_type_cache(element_type,
                                            TYPECACHE_CMP_PROC_FINFO);
    bool *nulls = NULL;
    int len = 0;
    Datum *array = NULL;
    deconstruct_array(arr,
                      element_type,
                      TI->typlen,
                      TI->typbyval,
                      TI->typalign,
                      &array,
                      &nulls,
                      &len);

    // filtering
    Datum *ret_array = (Datum *)palloc(len * sizeof(Datum));
    int count = 0;
    for (int i = 0; i < len; i ++) {
        float8 left  = datum_float8_cast(array[i], element_type);
        float8 right = datum_float8_cast(val     , element_type);
        bool pass = true;
        if (strcmp(op, "!=") == 0 || strcmp(op, "<>") == 0) {
            if (isnan(left) || isnan(right)) {
                pass = !(isnan(left) && isnan(right));
            } else { pass = (left != right); }
        } else if (strcmp(op, "=") == 0 || strcmp(op, "==") == 0) {
            if (isnan(left) || isnan(right)) {
                pass = (isnan(left) && isnan(right));
            } else { pass = (left == right); }
        } else if (strcmp(op, ">") == 0) {
            pass = (left > right);
        } else if (strcmp(op, ">=") == 0) {
            pass = (left >= right);
        } else if (strcmp(op, "<") == 0) {
            pass = (left < right);
        } else if (strcmp(op, "<=") == 0) {
            pass = (left <= right);
        } else {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("operator is not supported"),
                            errdetail("Filtering operator %s is not supported.", op)));
        }
        if (pass) { ret_array[count ++] = array[i]; }
    }

    // reconstruct
    ArrayType *ret = NULL;
    if (count == 0) {
        elog(WARNING, "array_filter: Returning empty array.");
        ret = construct_empty_array(element_type);
    } else {
        ret = construct_array(ret_array,
                              count,
                              element_type,
                              TI->typlen,
                              TI->typbyval,
                              TI->typalign);
    }

    pfree(array);
    pfree(ret_array);
    pfree(nulls);

    PG_RETURN_ARRAYTYPE_P(ret);
}

/*
 * This function normalizes an array as sum of squares to be 1.
 */

PG_FUNCTION_INFO_V1(array_normalize);
Datum
array_normalize(PG_FUNCTION_ARGS){
    ArrayType * arg = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(arg) != 1) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Input array with multiple dimensions is not allowed!")));
    }

    if (ARR_HASNULL(arg)) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("Input array with nulls is not allowed!")));
    }

    ArrayType *v = array_to_float8_array(arg);

    Datum norm_sqr = General_Array_to_Element(v, Float8GetDatum(0), 0.0,
            element_sum_sqr, noop_finalize);

    if (DatumGetFloat8(norm_sqr) == 0.) {
        elog(WARNING, "No non-zero elements found, returning the input array.");
        PG_RETURN_ARRAYTYPE_P(arg);
    }
    Datum inverse_norm = Float8GetDatum(1.0/sqrt(DatumGetFloat8(norm_sqr)));
    ArrayType* res = General_Array_to_Array(v, inverse_norm, element_mult);

    if (v != arg) { pfree(v); }
    PG_FREE_IF_COPY(arg,0);

    PG_RETURN_ARRAYTYPE_P(res);

}
/*
 * This function checks if an array contains NULL values.
 */
PG_FUNCTION_INFO_V1(array_contains_null);
Datum array_contains_null(PG_FUNCTION_ARGS){
    ArrayType *arg = PG_GETARG_ARRAYTYPE_P(0);
    if(ARR_HASNULL(arg)){
        return BoolGetDatum(true);
    } else {
        return BoolGetDatum(false);
    }
}


// ------------------------------------------------------------------------
// finalize functions
// ------------------------------------------------------------------------
static
inline
Datum
noop_finalize(Datum elt,int size,Oid element_type){
    (void) size; /* avoid warning about unused parameter */
    (void) element_type; /* avoid warning about unused parameter */
    return elt;
}

static
inline
Datum
average_finalize(Datum elt,int size,Oid element_type){
    float8 value = datum_float8_cast(elt, element_type);
     if (size == 0) {
        elog(WARNING, "Input array only contains NULL or NaN, returning 0");
        return Float8GetDatum(0);
    }
    return Float8GetDatum(value/(float8)size);
}

static
inline
Datum
value_index_finalize(void *mid_result,int size,Oid element_type) {
    Assert(mid_result);
	(void) element_type;
    (void) size;

    value_index *vi = (value_index *)mid_result;
    TypeCacheEntry *typentry = lookup_type_cache(FLOAT8OID, TYPECACHE_CMP_PROC_FINFO);
    int type_size = typentry->typlen;
    bool typbyval = typentry->typbyval;
    char typalign = typentry->typalign;

    Datum result[2];

   // We just use float8 as returned value to avoid overflow.
        result[0] = Float8GetDatum(vi->value);
        result[1] = Float8GetDatum((float8)(vi->index));

    // construct return result
    ArrayType *pgarray = construct_array(result,
                                 2,
                                 FLOAT8OID,
                                 type_size,
                                 typbyval,
                                 typalign);

    PG_RETURN_ARRAYTYPE_P(pgarray);
}

static
inline
Datum
average_root_finalize(Datum elt,int size,Oid element_type){
    float8 value = datum_float8_cast(elt, element_type);
    if (size == 0) {
        return Float8GetDatum(0);
    } else if (size == 1) {
        return Float8GetDatum(0);
    } else {
        return Float8GetDatum(sqrt(value/((float8)size - 1)));
    }
}

/* -----------------------------------------------------------------------
 * Type cast functions
 * -----------------------------------------------------------------------
*/
static
inline
int64
datum_int64_cast(Datum elt, Oid element_type) {
    switch(element_type){
        case INT2OID:
            return (int64) DatumGetInt16(elt); break;
        case INT4OID:
            return (int64) DatumGetInt32(elt); break;
        case INT8OID:
            return elt; break;
        default:
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("type is not supported"),
                            errdetail("Arrays with element type %s are not supported.",
                                      format_type_be(element_type))));
            break;
    }
    return 0;
}

static
inline
Datum
int64_datum_cast(int64 res, Oid result_type) {
    Datum result = Int64GetDatum(res);
    switch(result_type){
        case INT2OID:
            return DirectFunctionCall1(int82, result); break;
        case INT4OID:
            return DirectFunctionCall1(int84, result); break;
        case INT8OID:
            return result; break;
        default:
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("type is not supported"),
                            errdetail("Arrays with element type %s are not supported.",
                                      format_type_be(result_type))));
            break;
    }
    return result;
}
/* -----------------------------------------------------------------------
 * Type cast functions
 * -----------------------------------------------------------------------
*/
static
inline
float8
datum_float8_cast(Datum elt, Oid element_type) {
    switch(element_type){
        case INT2OID:
            return (float8) DatumGetInt16(elt); break;
        case INT4OID:
            return (float8) DatumGetInt32(elt); break;
        case INT8OID:
            return (float8) DatumGetInt64(elt); break;
        case FLOAT4OID:
            return (float8) DatumGetFloat4(elt); break;
        case FLOAT8OID:
            return (float8) DatumGetFloat8(elt); break;
        case NUMERICOID:
            return DatumGetFloat8(
                    DirectFunctionCall1(numeric_float8_no_overflow, elt));
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("type is not supported"),
                            errdetail("Arrays with element type %s are not supported.",
                                      format_type_be(element_type))));
            break;
    }
    return 0.0;
}

static
inline
Datum
float8_datum_cast(float8 res, Oid result_type) {
    Datum result = Float8GetDatum(res);
    switch(result_type){
        case INT2OID:
            return DirectFunctionCall1(dtoi2, result); break;
        case INT4OID:
            return DirectFunctionCall1(dtoi4, result); break;
        case INT8OID:
            return DirectFunctionCall1(dtoi8, result); break;
        case FLOAT4OID:
            return DirectFunctionCall1(dtof, result); break;
        case FLOAT8OID:
            return result; break;
        case NUMERICOID:
            return DirectFunctionCall1(float8_numeric, result); break;
        default:
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("type is not supported"),
                            errdetail("Arrays with element type %s are not supported.",
                                      format_type_be(result_type))));
            break;
    }
    return result;
}

/*
 *    Template for element-wise help functions
 */
static
inline
Datum
element_op(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type,
        float8 (*op)(float8, float8, float8)) {
    if (op == float8_div &&
            (result_type == INT2OID ||
             result_type == INT4OID ||
             result_type == INT8OID)) {
        int64 num   = datum_int64_cast(element, elt_type);
        int64 denom = datum_int64_cast(opt_elt, opt_type);
        return int64_datum_cast(int64_div(num, denom), result_type);
    }
    float8 elt = datum_float8_cast(element, elt_type   );
    float8 res = datum_float8_cast(result , result_type);
    float8 opt = datum_float8_cast(opt_elt, opt_type   );
    return float8_datum_cast((*op)(elt, res, opt), result_type);
}


// ------------------------------------------------------------------------
// element-wise helper functions
// ------------------------------------------------------------------------
/*
 * Add (elt1-flag)^2 to result.
 */

static
inline
Datum
element_diff(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type){
    return element_op(element, elt_type, result, result_type, opt_elt, opt_type, float8_diff);
}

/*
 * Add abs(elt1) to result.
 */
static
inline
Datum
element_abs_sum(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type){
    return element_op(element, elt_type, result, result_type, opt_elt, opt_type, float8_abs_sum);
}

/*
 * Add elt1 to result.
 */
static
inline
Datum
element_sum(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type){
    return element_op(element, elt_type, result, result_type, opt_elt, opt_type, float8_sum);
}

/*
 * Return min2(elt1, result).
 * First element if flag == 0
 */
static
inline
Datum
element_min(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type){
    return element_op(element, elt_type, result, result_type, opt_elt, opt_type, float8_min);
}

/*
 * Return max(element, index).
 */
static
inline
void*
element_argmax(Datum element, Oid elt_type, int elt_index, void *result) {
    Assert(result);
    value_index *vi = (value_index *)result;
    float8 elt = datum_float8_cast(element, elt_type);
    if (elt > vi->value) {
        vi->value = elt;
        vi->index = elt_index;
    }

    return vi;
}

/*
 * Return min(element, index).
 */
static
inline
void*
element_argmin(Datum element, Oid elt_type, int elt_index, void *result) {
    Assert(result);
    value_index *vi = (value_index *)result;
    float8 elt = datum_float8_cast(element, elt_type);
    if (elt < vi->value) {
        vi->value = elt;
        vi->index = elt_index;
    }

    return vi;
}

/*
 * Return max2(elt1, result).
 * First element if flag == 0
 */
static
inline
Datum
element_max(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type){
    return element_op(element, elt_type, result, result_type, opt_elt, opt_type, float8_max);
}

/*
 * Add elt1*elt2 to result.
 */
static
inline
Datum
element_dot(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type){
    return element_op(element, elt_type, result, result_type, opt_elt, opt_type, float8_dot);
}

/*
 * result++ if elt1 == elt2 or elt2 == 0.
 */
static
inline
Datum
element_contains(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type){
    return element_op(element, elt_type, result, result_type, opt_elt, opt_type, float8_contains);
}

/*
 * Assign result to be elt2.
 */
static
inline
Datum
element_set(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type){
    return element_op(element, elt_type, result, result_type, opt_elt, opt_type, float8_set);
}

/*
 * Assign result to be cos(elt1).
 */
static
inline
Datum
element_cos(Datum element, Oid elt_type, Datum result,
            Oid result_type, Datum opt_elt, Oid opt_type){
    return element_op(element, elt_type, result, result_type, opt_elt, opt_type, float8_cos);
}

/*
 * Assign result to be (elt1 + elt2).
 */
static
inline
Datum
element_add(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type){
    return element_op(element, elt_type, result, result_type, opt_elt, opt_type, float8_add);
}

/*
 * Assign result to be (elt1 - elt2).
 */
static
inline
Datum
element_sub(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type){
    return element_op(element, elt_type, result, result_type, opt_elt, opt_type, float8_sub);
}

/*
 * Assign result to be (elt1 * elt2).
 */
static
inline
Datum
element_mult(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type){
    return element_op(element, elt_type, result, result_type, opt_elt, opt_type, float8_mult);
}

/*
 * Assign result to be (elt1 / elt2).
 */
static
inline
Datum
element_div(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type){
    return element_op(element, elt_type, result, result_type, opt_elt, opt_type, float8_div);
}

/*
 * Assign result to be fabs(elt1).
 */
static
inline
Datum
element_abs(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type){
    return element_op(element, elt_type, result, result_type, opt_elt, opt_type, float8_abs);
}

/*
 * Assign result to be sqrt(elt1).
 */
static
inline
Datum
element_sqrt(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type){
    return element_op(element, elt_type, result, result_type, opt_elt, opt_type, float8_sqrt);
}

/*
 * Assign result to be power(elt1).
 */
static
inline
Datum
element_pow(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type){
    return element_op(element, elt_type, result, result_type, opt_elt, opt_type, float8_pow);
}

/*
 * Assign result to be square(elt1).
 */
static
inline
Datum
element_square(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type){
    return element_op(element, elt_type, result, result_type, opt_elt, opt_type, float8_square);
}

/*
 * add elt1 * elt1 to result.
 */
static
inline
Datum
element_sum_sqr(Datum element, Oid elt_type, Datum result,
        Oid result_type, Datum opt_elt, Oid opt_type){
    return element_op(element, elt_type, result, result_type, opt_elt, opt_type, float8_sum_sqr);
}
// ------------------------------------------------------------------------
// general helper functions
// ------------------------------------------------------------------------
/*
 * @brief Aggregates an array to a scalar.
 *
 * @param v Array.
 * @param exta_val An extra rgument for element_function.
 * @param element_function Transition function.
 * @param finalize_function Final function.
 * @returns Whatever finalize_function returns.
 */
Datum
General_Array_to_Element(
        ArrayType *v,
        Datum exta_val,
        float8 init_val,
        Datum(*element_function)(Datum,Oid,Datum,Oid,Datum,Oid),
        Datum(*finalize_function)(Datum,int,Oid)) {

    // dimensions
    int ndims = ARR_NDIM(v);
    if (ndims == 0) {
        elog(WARNING, "input are empty arrays.");
        return Float8GetDatum(0);
    }
    int *dims = ARR_DIMS(v);
    int nitems = ArrayGetNItems(ndims, dims);

    // type
    Oid element_type = ARR_ELEMTYPE(v);
    TypeCacheEntry *typentry = lookup_type_cache(element_type,TYPECACHE_CMP_PROC_FINFO);
    int type_size = typentry->typlen;
    bool typbyval = typentry->typbyval;
    char typalign = typentry->typalign;

    // iterate
    Datum result = Float8GetDatum(init_val);
    char *dat = ARR_DATA_PTR(v);
    int i = 0;
    int null_count = 0;
    if(ARR_HASNULL(v)){
        bits8 *bitmap = ARR_NULLBITMAP(v);
        int bitmask = 1;
        for (i = 0; i < nitems; i++) {
            /* Get elements, checking for NULL */
            if (!(bitmap && (*bitmap & bitmask) == 0)) {
                Datum elt = fetch_att(dat, typbyval, type_size);
                dat = att_addlength_pointer(dat, type_size, dat);
                dat = (char *) att_align_nominal(dat, typalign);

                // treating NaN as NULL
                if (!isnan(datum_float8_cast(elt,element_type))) {
                    result = element_function(elt,
                                              element_type,
                                              result,
                                              FLOAT8OID,
                                              exta_val,
                                              FLOAT8OID);
                } else {
                    null_count++;
                }
            }else{
                null_count++;
            }

            /* advance bitmap pointers if any */
            if (bitmap) {
                bitmask <<= 1;
                if (bitmask == 0x100) {
                    bitmap++;
                    bitmask = 1;
                }
            }
        }
    } else {
        for (i = 0; i < nitems; i++) {
            Datum elt = fetch_att(dat, typbyval, type_size);
            dat = att_addlength_pointer(dat, type_size, dat);
            dat = (char *) att_align_nominal(dat, typalign);

            // treating NaN as NULL
            if (!isnan(datum_float8_cast(elt,element_type))) {
                result = element_function(elt,
                                          element_type,
                                          result,
                                          FLOAT8OID,
                                          exta_val,
                                          FLOAT8OID);
            } else {
                null_count++;
            }
        }
    }

    return finalize_function(result,(nitems-null_count), FLOAT8OID);
}

// ------------------------------------------------------------------------
// general helper functions
// ------------------------------------------------------------------------
/*
 * @brief Aggregates an array to a composite struct.
 *
 * @param v Array.
 * @param init_val An initial value for element_function.
 * @param element_function Transition function.
 * @param finalize_function Final function.
 * @returns Whatever finalize_function returns.
 */
static Datum General_Array_to_Struct(ArrayType *v, void *init_val,
        void*(*element_function)(Datum,Oid,int,void*),
        Datum(*finalize_function)(void*,int,Oid)) {

    // dimensions
    int ndims = ARR_NDIM(v);
    if (ndims == 0) {
        elog(WARNING, "input are empty arrays.");
        return Float8GetDatum(0);
    }
    int *dims = ARR_DIMS(v);
    int nitems = ArrayGetNItems(ndims, dims);

    // type
    Oid element_type = ARR_ELEMTYPE(v);
    TypeCacheEntry *typentry = lookup_type_cache(element_type,TYPECACHE_CMP_PROC_FINFO);
    int type_size = typentry->typlen;
    bool typbyval = typentry->typbyval;
    char typalign = typentry->typalign;

    // iterate
    void* result = init_val;
    char *dat = ARR_DATA_PTR(v);
    int i = 0;
    int null_count = 0;
    int *lbs = ARR_LBOUND(v);
    if(ARR_HASNULL(v)){
        bits8 *bitmap = ARR_NULLBITMAP(v);
        int bitmask = 1;
        for (i = 0; i < nitems; i++) {
            /* Get elements, checking for NULL */
            if (!(bitmap && (*bitmap & bitmask) == 0)) {
                Datum elt = fetch_att(dat, typbyval, type_size);
                dat = att_addlength_pointer(dat, type_size, dat);
                dat = (char *) att_align_nominal(dat, typalign);

                // treating NaN as NULL
                if (!isnan(datum_float8_cast(elt,element_type))) {
                    result = element_function(elt,
                                              element_type,
                                              lbs[0] + i,
                                              result);
                } else {
                    null_count++;
                }
            }else{
                null_count++;
            }

            /* advance bitmap pointers if any */
            if (bitmap) {
                bitmask <<= 1;
                if (bitmask == 0x100) {
                    bitmap++;
                    bitmask = 1;
                }
            }
        }
    } else {
        for (i = 0; i < nitems; i++) {
            Datum elt = fetch_att(dat, typbyval, type_size);
            dat = att_addlength_pointer(dat, type_size, dat);
            dat = (char *) att_align_nominal(dat, typalign);

            // treating NaN as NULL
            if (!isnan(datum_float8_cast(elt,element_type))) {
                result = element_function(elt,
                                          element_type,
                                          lbs[0] + i,
                                          result);
            } else {
                null_count++;
            }
        }
    }

    return finalize_function(result,(nitems-null_count), element_type);
}

/*
 * @brief Aggregates two arrays to a scalar.
 *
 * @param v1 Array.
 * @param v2 Array.
 * @param element_function Transition function.
 * @param finalize_function Final function.
 * @returns Whatever finalize_function returns.
 */
Datum
General_2Array_to_Element(
        ArrayType *v1,
        ArrayType *v2,
        Datum(*element_function)(Datum,Oid,Datum,Oid,Datum,Oid),
        Datum(*finalize_function)(Datum,int,Oid)) {

    // dimensions
    int ndims1 = ARR_NDIM(v1);
    int ndims2 = ARR_NDIM(v2);
    if (ndims1 != ndims2) {
        ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                        errmsg("cannot perform operation arrays of different number of dimensions"),
                        errdetail("Arrays with %d and %d dimensions are not compatible for this opertation.",
                                  ndims1, ndims2)));
    }
    if (ndims2 == 0) {
        elog(WARNING, "input are empty arrays.");
        return Float8GetDatum(0);
    }
    int *lbs1 = ARR_LBOUND(v1);
    int *lbs2 = ARR_LBOUND(v2);
    int *dims1 = ARR_DIMS(v1);
    int *dims2 = ARR_DIMS(v2);
    int i = 0;
    for (i = 0; i < ndims1; i++) {
        if (dims1[i] != dims2[i] || lbs1[i] != lbs2[i]) {
            ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                            errmsg("cannot operate on arrays of different ranges of dimensions"),
                            errdetail("Arrays with range [%d,%d] and [%d,%d] for dimension %d are not compatible for operations.",
                                      lbs1[i], lbs1[i] + dims1[i], lbs2[i], lbs2[i] + dims2[i], i)));
        }
    }
    int nitems = ArrayGetNItems(ndims1, dims1);

    // nulls
    if (ARR_HASNULL(v1) || ARR_HASNULL(v2)) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("arrays cannot contain nulls"),
                        errdetail("Arrays with element value NULL are not allowed.")));
    }

    // type
    // the function signature guarantees v1 and v2 are of same type
    Oid element_type = ARR_ELEMTYPE(v1);
    TypeCacheEntry *typentry = lookup_type_cache(element_type,TYPECACHE_CMP_PROC_FINFO);
    int type_size = typentry->typlen;
    bool typbyval = typentry->typbyval;
    char typalign = typentry->typalign;

    // iterate
    Datum result = Float8GetDatum(0);
    char *dat1 = ARR_DATA_PTR(v1);
    char *dat2 = ARR_DATA_PTR(v2);
    for (i = 0; i < nitems; ++i) {
        Datum elt1 = fetch_att(dat1, typbyval, type_size);
        dat1 = att_addlength_pointer(dat1, type_size, dat1);
        dat1 = (char *) att_align_nominal(dat1, typalign);
        Datum elt2 = fetch_att(dat2, typbyval, type_size);
        dat2 = att_addlength_pointer(dat2, type_size, dat2);
        dat2 = (char *) att_align_nominal(dat2, typalign);

        result = element_function(elt1,
                                  element_type,
                                  result,
                                  FLOAT8OID,
                                  elt2,
                                  element_type);
    }

    return finalize_function(result, nitems, FLOAT8OID);
}

/*
 * @brief Maps two arrays to a single array.
 *
 * @param v1 Array.
 * @param v2 Array.
 * @param element_function Map function.
 * @returns Mapped array.
 */
ArrayType*
General_2Array_to_Array(
        ArrayType *v1,
        ArrayType *v2,
        Datum(*element_function)(Datum,Oid,Datum,Oid,Datum,Oid)) {

    // dimensions
    int ndims1 = ARR_NDIM(v1);
    int ndims2 = ARR_NDIM(v2);
    if (ndims1 != ndims2) {
        ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                        errmsg("cannot perform operation arrays of different number of dimensions"),
                        errdetail("Arrays with %d and %d dimensions are not compatible for this opertation.",
                                  ndims1, ndims2)));
    }
    if (ndims2 == 0) {
        elog(WARNING, "input are empty arrays.");
        return v1;
    }
    int *lbs1 = ARR_LBOUND(v1);
    int *lbs2 = ARR_LBOUND(v2);
    int *dims1 = ARR_DIMS(v1);
    int *dims2 = ARR_DIMS(v2);
    // for output array
    int ndims = ndims1;
    int *dims = (int *) palloc(ndims * sizeof(int));
    int *lbs = (int *) palloc(ndims * sizeof(int));
    int i = 0;
    for (i = 0; i < ndims; i++) {
        if (dims1[i] != dims2[i] || lbs1[i] != lbs2[i]) {
            ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                            errmsg("cannot operate on arrays of different ranges of dimensions"),
                            errdetail("Arrays with range [%d,%d] and [%d,%d] for dimension %d are not compatible for operations.",
                                      lbs1[i], lbs1[i] + dims1[i], lbs2[i], lbs2[i] + dims2[i], i)));
        }
        dims[i] = dims1[i];
        lbs[i] = lbs1[i];
    }
    int nitems = ArrayGetNItems(ndims, dims);

    // nulls
    if (ARR_HASNULL(v1) || ARR_HASNULL(v2)) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("arrays cannot contain nulls"),
                        errdetail("Arrays with element value NULL are not allowed.")));
    }

    // type
    // the function signature guarantees v1 and v2 are of same type
    Oid element_type = ARR_ELEMTYPE(v1);
    TypeCacheEntry *typentry = lookup_type_cache(element_type,TYPECACHE_CMP_PROC_FINFO);
    int type_size = typentry->typlen;
    bool typbyval = typentry->typbyval;
    char typalign = typentry->typalign;

    // allocate
    Datum *result = NULL;
    switch (element_type) {
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
            result = (Datum *)palloc(nitems * sizeof(Datum));break;
        default:
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("type is not supported"),
                            errdetail("Arrays with element type %s are not supported.",
                                      format_type_be(element_type))));
            break;
    }

    // iterate
    Datum *resultp = result;
    char *dat1 = ARR_DATA_PTR(v1);
    char *dat2 = ARR_DATA_PTR(v2);
    for (i = 0; i < nitems; ++i) {
        Datum elt1 = fetch_att(dat1, typbyval, type_size);
        dat1 = att_addlength_pointer(dat1, type_size, dat1);
        dat1 = (char *) att_align_nominal(dat1, typalign);

        Datum elt2 = fetch_att(dat2, typbyval, type_size);
        dat2 = att_addlength_pointer(dat2, type_size, dat2);
        dat2 = (char *) att_align_nominal(dat2, typalign);

        *resultp = element_function(elt1,
                                    element_type,
                                    elt1,         /* placeholder */
                                    element_type, /* placeholder */
                                    elt2,
                                    element_type);
        resultp ++;
    }


    // construct return result
    ArrayType *pgarray = construct_md_array(result,
                                 NULL,
                                 ndims,
                                 dims,
                                 lbs,
                                 element_type,
                                 type_size,
                                 typbyval,
                                 typalign);

    pfree(result);
    pfree(dims);
    pfree(lbs);

    return pgarray;
}

/*
 * @brief Transforms an array.
 *
 * @param v1 Array.
 * @param elt2 Parameter.
 * @param element_function Map function.
 * @returns Transformed array.
 */
ArrayType*
General_Array_to_Array(
        ArrayType *v1,
        Datum elt2,
        Datum(*element_function)(Datum,Oid,Datum,Oid,Datum,Oid)) {

    // dimensions
    int ndims1 = ARR_NDIM(v1);
    if (ndims1 == 0) {
        elog(WARNING, "input are empty arrays.");
        return v1;
    }
    int ndims = ndims1;
    int *lbs1 = ARR_LBOUND(v1);
    int *dims1 = ARR_DIMS(v1);
    int *dims = (int *) palloc(ndims * sizeof(int));
    int *lbs = (int *) palloc(ndims * sizeof(int));
    int i = 0;
    for (i = 0; i < ndims; i ++) {
        dims[i] = dims1[i];
        lbs[i] = lbs1[i];
    }
    int nitems = ArrayGetNItems(ndims, dims);

    // nulls
    if (ARR_HASNULL(v1)) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("arrays cannot contain nulls"),
                        errdetail("Arrays with element value NULL are not allowed.")));
    }

    // type
    Oid element_type = ARR_ELEMTYPE(v1);
    TypeCacheEntry *typentry = lookup_type_cache(element_type,TYPECACHE_CMP_PROC_FINFO);
    int type_size = typentry->typlen;
    bool typbyval = typentry->typbyval;
    char typalign = typentry->typalign;

    // allocate
    Datum *result = NULL;
    switch (element_type) {
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
            result = (Datum *)palloc(nitems * sizeof(Datum));break;
        default:
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("type is not supported"),
                            errdetail("Arrays with element type %s are not supported.",
                                      format_type_be(element_type))));
            break;
    }

    // iterate
    Datum *resultp = result;
    char *dat1 = ARR_DATA_PTR(v1);
    for (i = 0; i < nitems; i ++) {
        // iterate elt1
        Datum elt1 = fetch_att(dat1, typbyval, type_size);
        dat1 = att_addlength_pointer(dat1, type_size, dat1);
        dat1 = (char *) att_align_nominal(dat1, typalign);

        *resultp = element_function(elt1,
                                    element_type,
                                    elt1,         /* placeholder */
                                    element_type, /* placeholder */
                                    elt2,
                                    element_type);
        resultp ++;
    }

    // construct return result
    ArrayType *pgarray = construct_md_array(result,
                                            NULL,
                                            ndims,
                                            dims,
                                            lbs,
                                            element_type,
                                            type_size,
                                            typbyval,
                                            typalign);

    pfree(result);
    pfree(dims);
    pfree(lbs);

    return pgarray;
}


/*
 * @brief Transforms an array to another array using cumulative operations.
 *
 * @param v1 Array.
 * @param initial Parameter.
 * @param element_function Map function.
 * @returns Transformed array.
 */
ArrayType*
General_Array_to_Cumulative_Array(
        ArrayType *v1,
        Datum initial,
        Datum(*element_function)(Datum,Oid,Datum,Oid,Datum,Oid)) {

    // dimensions
    int ndims1 = ARR_NDIM(v1);
    if (ndims1 == 0) {
        elog(WARNING, "input are empty arrays.");
        return v1;
    }
    int ndims = ndims1;
    int *lbs1 = ARR_LBOUND(v1);
    int *dims1 = ARR_DIMS(v1);
    int *dims = (int *) palloc(ndims * sizeof(int));
    int *lbs = (int *) palloc(ndims * sizeof(int));
    int i = 0;
    for (i = 0; i < ndims; i ++) {
        dims[i] = dims1[i];
        lbs[i] = lbs1[i];
    }
    int nitems = ArrayGetNItems(ndims, dims);

    // nulls
    if (ARR_HASNULL(v1)) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("arrays cannot contain nulls"),
                        errdetail("Arrays with element value NULL are not allowed.")));
    }

    // type
    Oid element_type = ARR_ELEMTYPE(v1);
    TypeCacheEntry *typentry = lookup_type_cache(element_type,TYPECACHE_CMP_PROC_FINFO);
    int type_size = typentry->typlen;
    bool typbyval = typentry->typbyval;
    char typalign = typentry->typalign;

    // allocate
    Datum *result = NULL;
    switch (element_type) {
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
            result = (Datum *)palloc(nitems * sizeof(Datum));break;
        default:
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("type is not supported"),
                            errdetail("Arrays with element type %s are not supported.",
                                      format_type_be(element_type))));
            break;
    }

    // iterate
    Datum *resultp = result;
    char *dat1 = ARR_DATA_PTR(v1);
    float prev_dat = DatumGetFloat8(initial);
    for (i = 0; i < nitems; i ++) {
        // iterate elt1
        Datum elt1 = fetch_att(dat1, typbyval, type_size);
        dat1 = att_addlength_pointer(dat1, type_size, dat1);
        dat1 = (char *) att_align_nominal(dat1, typalign);

        *resultp = element_function(elt1,
                                    element_type,
                                    elt1,         /* placeholder */
                                    element_type, /* placeholder */
                                    prev_dat,
                                    element_type);
        prev_dat = *resultp;
        resultp++;
    }

    // construct return result
    ArrayType *pgarray = construct_md_array(result,
                                            NULL,
                                            ndims,
                                            dims,
                                            lbs,
                                            element_type,
                                            type_size,
                                            typbyval,
                                            typalign);

    pfree(result);
    pfree(dims);
    pfree(lbs);

    return pgarray;
}

//======= Functions below all needed for my_array_concat_transition()

#define ARRNELEMS(x)  ArrayGetNItems(ARR_NDIM(x), ARR_DIMS(x))

ArrayType *expand_if_needed(ArrayType *a, unsigned long new_bytes)
{
    unsigned long data_size, current_size;
    unsigned long ndims, current_space, new_space;
    ArrayType *r=a;

    data_size = sizeof(float4) * ARRNELEMS(a);
    ndims = ARR_NDIM(a);
    current_size = ARR_OVERHEAD_NONULLS(ndims) + data_size;
    current_space = VARSIZE(a);

    if (current_size + new_bytes > current_space) {
        new_space = 2*current_space - ARR_OVERHEAD_NONULLS(ndims);  // If already half full, double
                                               // size of array allocated
//        ereport(INFO, (errmsg("current size is %lu, so expanding space from %lu to %lu.", current_size, current_space, new_space)));
        r = palloc(new_space);
        memcpy(r, a, current_size);
        SET_VARSIZE(r, new_space);  // important!  postgres will crash at pfree otherwise
    }

    return r;
}

// ----------------------------------------------------------------------

PG_FUNCTION_INFO_V1(my_array_concat_transition);
Datum my_array_concat_transition(PG_FUNCTION_ARGS)
{
    // args 0 = state
    // args 1 = array of float4's
    // args 2 = max rows on this segment (okay if larger, just can't be smaller)
    ArrayType  *state, *old_state;
    float4 *b_data;
    float4 *state_data;
    unsigned long num_current_elems, num_new_elems, num_elements;
    unsigned long current_bytes, new_bytes;
    ArrayType *b = PG_GETARG_ARRAYTYPE_P(1);
    MemoryContext agg_context;

    if (PG_ARGISNULL(0)) {
        state = NULL;
    } else {
        state = PG_GETARG_ARRAYTYPE_P(0);
    }

    if (AggCheckCallContext(fcinfo, &agg_context)) {
        if (!state) { // first row
//			new_context = AllocSetContextCreate(agg_context,
//											"my_array_concat_transition",
//											ALLOCSET_DEFAULT_SIZES);
//			MemoryContextSwitchTo(agg_context);
            num_new_elems = ARRNELEMS(b);
            state = expand_if_needed(b, num_new_elems * sizeof(float4));
//            ereport(INFO, (errmsg("read first row")));
            PG_RETURN_POINTER(state);
        } else {
            num_new_elems = ARRNELEMS(b);
            current_bytes = VARSIZE(state);
            old_state = state;
    //            state = repalloc(state, nbytes + num_new_elems*(sizeof(float4)));
            state = expand_if_needed(state, num_new_elems * sizeof(float4));
            if (state == NULL) {
                ereport(ERROR, (errmsg("repalloc returned NULL pointer!")));
            } else if (state == old_state) {
//                ereport(INFO, (errmsg("repalloc returned same pointer")));
            } else {
//                ereport(INFO, (errmsg("repalloc returned new pointer")));
            }
         }
    } else {
        num_new_elems = ARRNELEMS(b);
        ereport(INFO, (errmsg("non-agg context!")));
        if (PG_ARGISNULL(0)) {
            ereport(ERROR, (errmsg("First operand must be non-null")));
        }
        ArrayType *a = PG_GETARG_ARRAYTYPE_P(0);
        num_new_elems = ARRNELEMS(b);
        current_bytes = VARSIZE(a);
        new_bytes = current_bytes + num_new_elems*(sizeof(float4));
        state = palloc(new_bytes);
        memcpy(state, a, current_bytes);
        SET_VARSIZE(state, new_bytes);
    }

    if (ARR_NDIM(state) != ARR_NDIM(b)) {
        ereport(ERROR, (errmsg("All arrays must have the same number of dimensions")));
    }

    num_current_elems = ARRNELEMS(state);

//    ereport(INFO, (errmsg("num_current_elems = %d", num_current_elems)));
//    ereport(INFO, (errmsg("num_new_elems = %d", num_new_elems)));

    state_data = (float4 *) ARR_DATA_PTR(state);
    b_data = (float4 *) ARR_DATA_PTR(b);

    num_elements = num_current_elems + num_new_elems;
    memcpy(state_data + num_current_elems, b_data, num_new_elems * sizeof(float4));

//    ereport(INFO, (errmsg("state dims = [%d, %d, %d]", ARR_DIMS(state)[0], ARR_DIMS(state)[1],ARR_DIMS(state)[2])));

    ARR_DIMS(state)[0] = ARR_DIMS(state)[0] + ARR_DIMS(b)[0];

//    ereport(INFO, (errmsg("Returning state")));
    PG_RETURN_POINTER(state);
}

//========================== BackPort of postgresql array_agg() function ============

/*
  * working state for accumArrayResultArr() and friends
  * note that the input must be arrays, and the same array type is returned
  */
 typedef struct ArrayBuildStateArr
 {
     MemoryContext mcontext;     /* where all the temp stuff is kept */
     char       *data;           /* accumulated data */
     bits8      *nullbitmap;     /* bitmap of is-null flags, or NULL if none */
     int         abytes;         /* allocated length of "data" */
     int         nbytes;         /* number of bytes used so far */
     int         aitems;         /* allocated length of bitmap (in elements) */
     int         nitems;         /* total number of elements in result */
     int         ndims;          /* current dimensions of result */
     int         dims[MAXDIM];
     int         lbs[MAXDIM];
     Oid         array_type;     /* data type of the arrays */
     Oid         element_type;   /* data type of the array elements */
     bool        private_cxt;    /* use private memory context */
 } ArrayBuildStateArr;

#define ALLOCSET_DEFAULT_SIZES \
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE


// TODO:  we should not be defining this here... must be some other
 ///  way to get definition that's in mpool.h of gpdb source
struct MPool
{
    MemoryContextData *parent;
    MemoryContextData *context;

    /*
     * Total number of bytes are allocated through the memory
     * context.
     */
    uint64 total_bytes_allocated;

    /* How many bytes are used by the caller. */
    uint64 bytes_used;

    /*
     * When a new allocation request arrives, and the current block
     * does not have enough space for this request, we waste those
     * several bytes at the end of the block. This variable stores
     * total number of these wasted bytes.
     */
    uint64 bytes_wasted;

    /* The latest allocated block of available space. */
    void *start;
    void *end;
};

PG_FUNCTION_INFO_V1(array_agg_array_serialize);
Datum array_agg_array_serialize(PG_FUNCTION_ARGS)
{
    ArrayBuildStateArr *astate = (ArrayBuildStateArr *) PG_GETARG_POINTER(0);
    elog(INFO, "ser astate->ndims: %d", astate->ndims);
    unsigned long bytea_data_size = sizeof(ArrayBuildStateArr) + astate->nbytes;
    astate->abytes = astate->nbytes;

    bytea *b = palloc(VARHDRSZ + bytea_data_size);
    SET_VARSIZE(b, VARHDRSZ + bytea_data_size);
    memcpy(VARDATA_ANY(b), astate, bytea_data_size);

    PG_RETURN_BYTEA_P(b);
};

PG_FUNCTION_INFO_V1(array_agg_array_deserialize);
Datum array_agg_array_deserialize(PG_FUNCTION_ARGS)
{
    bytea *b = PG_GETARG_BYTEA_P(0);

    unsigned long bytea_data_size = VARSIZE(b) - VARHDRSZ;

    int ndims = ((ArrayBuildStateArr *) VARDATA_ANY(b))->ndims;
    elog(INFO, "des local ndims: %d", ndims);
    unsigned long overhead = sizeof(ArrayBuildStateArr);
    ArrayBuildStateArr *astate = palloc(overhead);
    memcpy(astate, VARDATA_ANY(b), overhead);
    elog(INFO, "des after overhead copy astate->ndims: %d", astate->ndims);

    astate->data = palloc(bytea_data_size-overhead);
    memcpy(astate->data, VARDATA_ANY(b)+overhead, bytea_data_size-overhead);

    // astate->data = (char *)astate + ARR_OVERHEAD_NONULLS(astate->ndims);
    elog(INFO, "des astate->ndims: %d", astate->ndims);
    astate->mcontext = CurrentMemoryContext;
    PG_RETURN_POINTER(astate);
};

 /*
  * The following three functions provide essentially the same API as
  * initArrayResult/accumArrayResult/makeArrayResult, but instead of accepting
  * inputs that are array elements, they accept inputs that are arrays and
  * produce an output array having N+1 dimensions.  The inputs must all have
  * identical dimensionality as well as element type.
  */

 /*
  * initArrayResultArr - initialize an empty ArrayBuildStateArr
  *
  *  array_type is the array type (must be a valid varlena array type)
  *  element_type is the type of the array's elements (lookup if InvalidOid)
  *  rcontext is where to keep working state
  *  subcontext is a flag determining whether to use a separate memory context
  */
 ArrayBuildStateArr *
 initArrayResultArr(Oid array_type, Oid element_type, MemoryContext rcontext,
                    bool subcontext)
 {
     ArrayBuildStateArr *astate;
     MemoryContext arr_context = rcontext;   /* by default use the parent ctx */
    //     MPool *mpool;    // Maybe we should be saving this somewhere? Right now,
                        //   it's just temporary

     /* Lookup element type, unless element_type already provided */
     if (!OidIsValid(element_type))
     {
         element_type = get_element_type(array_type);

         if (!OidIsValid(element_type))
             ereport(ERROR,
                     (errcode(ERRCODE_DATATYPE_MISMATCH),
                      errmsg("data type %s is not an array type",
                             format_type_be(array_type))));
     }

     /* Make a temporary context to hold all the junk */

     if (subcontext)
         arr_context = AllocSetContextCreate(rcontext,
                                             "accumArrayResultArr",
                                             ALLOCSET_DEFAULT_SIZES);
    //        mpool = mpool_create(arr_context, "accumArrayResultArr");
    //        arr_context = mpool->context;

     /* Note we initialize all fields to zero */
     astate = (ArrayBuildStateArr *)
         MemoryContextAllocZero(arr_context, sizeof(ArrayBuildStateArr));
    //           mpool_alloc(arr_context, sizeof(ArrayBuildStateArr));
    //           MemSet(arr_context, sizeof(ArrayBuildStateArr));
     astate->mcontext = arr_context;
     astate->private_cxt = subcontext;

     /* Save relevant datatype information */
     astate->array_type = array_type;
     astate->element_type = element_type;

     return astate;
}

/*
  * accumArrayResultArr - accumulate one (more) sub-array for an array result
  *
  *  astate is working state (can be NULL on first call)
  *  dvalue/disnull represent the new sub-array to append to the array
  *  array_type is the array type (must be a valid varlena array type)
  *  rcontext is where to keep working state
  */
 ArrayBuildStateArr *
 accumArrayResultArr(ArrayBuildStateArr *astate,
                     Datum dvalue, bool disnull,
                     Oid array_type,
                     MemoryContext rcontext)
 {
     ArrayType  *arg;
     MemoryContext oldcontext;
     int        *dims,
                *lbs,
                 ndims,
                 nitems,
                 ndatabytes;
     char       *data;
     int         i;

     /*
      * We disallow accumulating null subarrays.  Another plausible definition
      * is to ignore them, but callers that want that can just skip calling
      * this function.
      */
     if (disnull)
         ereport(ERROR,
                 (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                  errmsg("cannot accumulate null arrays")));

     /* Detoast input array in caller's context */
     arg = DatumGetArrayTypeP(dvalue);

     if (astate == NULL)
         astate = initArrayResultArr(array_type, InvalidOid, rcontext, true);
     else
         Assert(astate->array_type == array_type);

     oldcontext = MemoryContextSwitchTo(astate->mcontext);

     /* Collect this input's dimensions */
     ndims = ARR_NDIM(arg);
     dims = ARR_DIMS(arg);
     lbs = ARR_LBOUND(arg);
     data = ARR_DATA_PTR(arg);
     nitems = ArrayGetNItems(ndims, dims);
     ndatabytes = ARR_SIZE(arg) - ARR_DATA_OFFSET(arg);

     if (astate->ndims == 0)
     {
         /* First input; check/save the dimensionality info */

         /* Should we allow empty inputs and just produce an empty output? */
         if (ndims == 0)
             ereport(ERROR,
                     (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                      errmsg("cannot accumulate empty arrays")));
         if (ndims + 1 > MAXDIM)
             ereport(ERROR,
                     (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                      errmsg("number of array dimensions (%d) exceeds the maximum allowed (%d)",
                             ndims + 1, MAXDIM)));

         /*
          * The output array will have n+1 dimensions, with the ones after the
          * first matching the input's dimensions.
          */
         astate->ndims = ndims + 1;
         astate->dims[0] = 0;
         memcpy(&astate->dims[1], dims, ndims * sizeof(int));
         astate->lbs[0] = 1;
         memcpy(&astate->lbs[1], lbs, ndims * sizeof(int));

         /* Allocate at least enough data space for this item */
         astate->abytes = 1024;
         while (astate->abytes <= ndatabytes)
             astate->abytes *= 2;
         astate->data = (char *) palloc(astate->abytes);
     }
     else
     {
         /* Second or later input: must match first input's dimensionality */
         if (astate->ndims != ndims + 1)
             ereport(ERROR,
                     (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                      errmsg("cannot accumulate arrays of different dimensionality")));
         for (i = 0; i < ndims; i++)
         {
             if (astate->dims[i + 1] != dims[i] || astate->lbs[i + 1] != lbs[i])
                 ereport(ERROR,
                         (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                          errmsg("cannot accumulate arrays of different dimensionality")));
         }

         /* Enlarge data space if needed */
         if (astate->nbytes + ndatabytes > astate->abytes)
         {
             astate->abytes = Max(astate->abytes * 2,
                                  astate->nbytes + ndatabytes);
             astate->data = (char *) repalloc(astate->data, astate->abytes);
         }
     }

     /*
      * Copy the data portion of the sub-array.  Note we assume that the
      * advertised data length of the sub-array is properly aligned.  We do not
      * have to worry about detoasting elements since whatever's in the
      * sub-array should be OK already.
      */
     memcpy(astate->data + astate->nbytes, data, ndatabytes);
     astate->nbytes += ndatabytes;

     /* Deal with null bitmap if needed */
     if (astate->nullbitmap || ARR_HASNULL(arg))
     {
         int         newnitems = astate->nitems + nitems;

         if (astate->nullbitmap == NULL)
         {
             /*
              * First input with nulls; we must retrospectively handle any
              * previous inputs by marking all their items non-null.
              */
             astate->aitems = 256;
             while (astate->aitems <= newnitems)
                 astate->aitems *= 2;
             astate->nullbitmap = (bits8 *) palloc((astate->aitems + 7) / 8);
             array_bitmap_copy(astate->nullbitmap, 0,
                               NULL, 0,
                               astate->nitems);
         }
         else if (newnitems > astate->aitems)
         {
             astate->aitems = Max(astate->aitems * 2, newnitems);
             astate->nullbitmap = (bits8 *)
                 repalloc(astate->nullbitmap, (astate->aitems + 7) / 8);
         }
         array_bitmap_copy(astate->nullbitmap, astate->nitems,
                           ARR_NULLBITMAP(arg), 0,
                           nitems);
     }

     astate->nitems += nitems;
     astate->dims[0] += 1;

     MemoryContextSwitchTo(oldcontext);

     /* Release detoasted copy if any */
     if ((Pointer) arg != DatumGetPointer(dvalue))
         pfree(arg);

     return astate;
}


/*
  * ARRAY_AGG(anyarray) aggregate function
  */
PG_FUNCTION_INFO_V1(array_agg_array_transfn);
Datum
array_agg_array_transfn(PG_FUNCTION_ARGS)
{
     Oid         arg1_typeid = get_fn_expr_argtype(fcinfo->flinfo, 1);
     MemoryContext aggcontext;
     ArrayBuildStateArr *state;

     if (arg1_typeid == InvalidOid)
         ereport(ERROR,
                 (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                  errmsg("could not determine input data type")));

     /*
      * Note: we do not need a run-time check about whether arg1_typeid is a
      * valid array type, because the parser would have verified that while
      * resolving the input/result types of this polymorphic aggregate.
      */

     if (!AggCheckCallContext(fcinfo, &aggcontext))
     {
         /* cannot be called directly because of internal-type argument */
         elog(ERROR, "array_agg_array_transfn called in non-aggregate context");
     }


     if (PG_ARGISNULL(0))
         state = initArrayResultArr(arg1_typeid, InvalidOid, aggcontext, false);
     else
         state = (ArrayBuildStateArr *) PG_GETARG_POINTER(0);

     state = accumArrayResultArr(state,
                                 PG_GETARG_DATUM(1),
                                 PG_ARGISNULL(1),
                                 arg1_typeid,
                                 aggcontext);

     /*
      * The transition type for array_agg() is declared to be "internal", which
      * is a pass-by-value type the same size as a pointer.  So we can safely
      * pass the ArrayBuildStateArr pointer through nodeAgg.c's machinations.
      */
     PG_RETURN_POINTER(state);
 }

// swaps state1 and state2
static inline void swap_states(ArrayBuildStateArr **state1, ArrayBuildStateArr **state2)
{
    ArrayBuildStateArr *state_temp = *state1;
    *state1 = *state2;
    *state2 = state_temp;
}

/*
  * mergeArrayResultArr - concatenate two arrays into a single array
  *
  *  state1 and state2 are the two states to be merged.  If either is
  *  NULL then the other one is returned.  Otherwise, arrays are
  *  concatenated into a single array where first dimension is the sum
  *  of the two first dimensions, and rest of the dimensions are left
  *  untouched.
  *
  */
ArrayBuildStateArr *
mergeArrayResultArr(ArrayBuildStateArr *state1,
                 ArrayBuildStateArr *state2,
                 Oid array_type,
                 MemoryContext rcontext)
{
    MemoryContext oldcontext;
    int         i;

    Assert(state1->array_type == array_type);
    Assert(state2->array_type == array_type);
    Assert(state1->mcontext == state2->mcontext);

    oldcontext = MemoryContextSwitchTo(state1->mcontext);

    if (state1->ndims != state2->ndims){
        ereport(ERROR,
                (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                 errmsg("cannot merge arrays of different dimensionality1")));
    }
    for (i = 1; i < state1->ndims; i++)
    {
        if (state1->dims[i] != state2->dims[i] || state1->lbs[i] != state2->lbs[i])
            ereport(ERROR,
                    (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                     errmsg("cannot merge arrays of different dimensionality2")));
    }

    // swap states if necessary, to make sure state1 is the state with
    //   the larger amount of space already allocated (where we will
    //   store the result)

    if (state2->abytes > state1->abytes) {
        swap_states(&state1, &state2);
    }

    /* Enlarge state1's available space further if needed */
    if (state1->nbytes + state2->nbytes > state1->abytes)
    {
        state1->abytes = state1->abytes * 2;
        /* TODO: Replace with something like: state1->data = (char *) mpool(state1->data, state1->abytes); */
        /*   or whatever repalloc equiv is for mpool */

        state1->data = (char *) repalloc(state1->data, state1->abytes);
    }

    /* append state2 data onto the end of state1 data */
    memcpy(state1->data + state1->nbytes, state2->data, state2->nbytes);

    state1->nbytes += state2->nbytes;
    state1->nitems += state2->nitems;
    state1->dims[0] += state2->dims[0];

    MemoryContextSwitchTo(oldcontext);
    return state1;
}
/*
  * array_agg_array merge function
  */
PG_FUNCTION_INFO_V1(array_agg_array_mergefn);
Datum
array_agg_array_mergefn(PG_FUNCTION_ARGS)
{
    Oid         arg1_typeid = get_fn_expr_argtype(fcinfo->flinfo, 1);
    MemoryContext aggcontext;
    ArrayBuildStateArr *state1, *state2;

    if (arg1_typeid == InvalidOid)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("could not determine input data type")));

    /*
     * Note: we do not need a run-time check about whether arg1_typeid is a
     * valid array type, because the parser would have verified that while
     * resolving the input/result types of this polymorphic aggregate.
     */

    if (!AggCheckCallContext(fcinfo, &aggcontext))
    {
        /* cannot be called directly because of internal-type argument */
        elog(ERROR, "array_agg_array_mergefn called in non-aggregate context");
    }

    state1 = (ArrayBuildStateArr *) PG_GETARG_POINTER(0);
    state2 = (ArrayBuildStateArr *) PG_GETARG_POINTER(1);

    if (!state1 || PG_ARGISNULL(0)) {
        if (! state2 || PG_ARGISNULL(1)) {
            PG_RETURN_NULL();    /* return null if both inputs are null */
        }
        PG_RETURN_POINTER(state2);  // return state2 if state1 is null
    } else if (!state2 || PG_ARGISNULL(1)) {
        PG_RETURN_POINTER(state1); // return state1 if state2 is null
    }

    // do actual merge only when neither state is null
    state1 = mergeArrayResultArr(state1,
                                 state2,
                                 arg1_typeid,
                                 aggcontext);

    /*
     * The transition type for array_agg() is declared to be "internal", which
     * is a pass-by-value type the same size as a pointer.  So we can safely
     * pass the ArrayBuildStateArr pointer through nodeAgg.c's machinations.
     */
    PG_RETURN_POINTER(state1);
 }

/*
  * makeArrayResultArr - produce N+1-D final result of accumArrayResultArr
  *
  *  astate is working state (must not be NULL)
  *  rcontext is where to construct result
  *  release is true if okay to release working state
  */
 Datum
 makeArrayResultArr(ArrayBuildStateArr *astate,
                   MemoryContext rcontext,
                   bool release)
{
    ArrayType  *result;
    MemoryContext oldcontext;

    /* Build the final array result in rcontext */
    oldcontext = MemoryContextSwitchTo(rcontext);

    if (astate->ndims == 0)
    {
        /* No inputs, return empty array */
        result = construct_empty_array(astate->element_type);
    }
    else
    {
        int         dataoffset,
                    nbytes;

        /* Compute required space */
        nbytes = astate->nbytes;
        if (astate->nullbitmap != NULL)
        {
            dataoffset = ARR_OVERHEAD_WITHNULLS(astate->ndims, astate->nitems);
            nbytes += dataoffset;
        }
        else
        {
            dataoffset = 0;
            nbytes += ARR_OVERHEAD_NONULLS(astate->ndims);
        }

        result = (ArrayType *) palloc0(nbytes);
        SET_VARSIZE(result, nbytes);
        result->ndim = astate->ndims;
        result->dataoffset = dataoffset;
        result->elemtype = astate->element_type;

        memcpy(ARR_DIMS(result), astate->dims, astate->ndims * sizeof(int));
        memcpy(ARR_LBOUND(result), astate->lbs, astate->ndims * sizeof(int));
        memcpy(ARR_DATA_PTR(result), astate->data, astate->nbytes);

        if (astate->nullbitmap != NULL)
            array_bitmap_copy(ARR_NULLBITMAP(result), 0,
                              astate->nullbitmap, 0,
                              astate->nitems);
    }

    MemoryContextSwitchTo(oldcontext);

    /* Clean up all the junk */
    if (release)
    {
        Assert(astate->private_cxt);
        MemoryContextDelete(astate->mcontext);
    }

    return PointerGetDatum(result);
}

/* final func for array_agg_array */
PG_FUNCTION_INFO_V1(array_agg_array_finalfn);
Datum
array_agg_array_finalfn(PG_FUNCTION_ARGS)
{
    Datum       result;
    ArrayBuildStateArr *state;

    /* cannot be called directly because of internal-type argument */
    Assert(AggCheckCallContext(fcinfo, NULL));

    state = PG_ARGISNULL(0) ? NULL : (ArrayBuildStateArr *) PG_GETARG_POINTER(0);

    if (state == NULL)
        PG_RETURN_NULL();       /* returns null iff no input values */

    /*
     * Make the result.  We cannot release the ArrayBuildStateArr because
     * sometimes aggregate final functions are re-executed.  Rather, it is
     * nodeAgg.c's responsibility to reset the aggcontext when it's safe to do
     * so.
     */
    result = makeArrayResultArr(state, CurrentMemoryContext, false);

    PG_RETURN_DATUM(result);
}
