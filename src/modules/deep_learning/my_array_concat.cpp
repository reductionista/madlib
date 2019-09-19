#include <dbconnector/dbconnector.hpp>

#include "my_array_concat.hpp"

namespace madlib {

namespace modules {

namespace deep_learning {

using namespace dbal::eigen_integration;

// ----------------------------------------------------------------------

AnyType
my_array_concat_transition::run(AnyType& args) {

PG_FUNCTION_INFO_V1(my_array_concat_transition);
Datum           my_array_concat_transition((PG_FUNCTION_ARGS);
    // args 0 = state
    // args 1 = array of doubles
    //
    ArrayType  *a, *b;
    float8 *da,
    float8 *db;
    int i, n;

    b = PG_GETARG_ARRAYTYPE_P(1);
    CHECKARRVALID(b);

    if (AggCheckCallContext(fcinfo, NULL))
        {
            // Called in aggregate context...
            if (PG_ARGISNULL(0))
                // ... for the first time in a run, so the state in the 1st
                // argument is null. Create a state-holder array by copying the
                // second input array and return it.
                PG_RETURN_POINTER(copy_intArrayType(b));
            else
                // ... for a later invocation in the same run, so we'll modify
                // the state array directly.
                a = PG_GETARG_ARRAYTYPE_P(0);
        }
    else {


    }

    if (args[1].isNull() ){
        throw std::runtime_error("We need the x");
    }
    double *state;
    double *x = args[1].getAs<double *>
    int size = 3072;
    int state_size = 700*1024*1024;
    int count;

    if (args[0].isNull()){
        state = palloc(state_size);
        count = 1;
    } else {
        state = args[0].getAs<double *>
        count = state[0]
   }

    for (int j=count; j < size; j++) {
        state[count+j] = x[j];
    }

    state[0] = count;

    return state;

}} // deep_learning

} // modules

} // madlib
