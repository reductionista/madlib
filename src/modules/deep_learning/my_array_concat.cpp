#include <dbconnector/dbconnector.hpp>

#include "my_array_concat.hpp"

namespace madlib {

namespace modules {

namespace deep_learning {

using namespace dbal::eigen_integration;

// ----------------------------------------------------------------------

AnyType
my_array_concat_transition::run(AnyType& args) {
    // args 0 = state
    // args 1 = array of doubles
    //
    int i=0;

    if (args[1].isNull() ){
        throw std::runtime_error("We need the x");
    }
    MutableNativeColumnVector x = args[1].getAs<MutableNativeColumnVector>();
    Index old_state_size;
    Index size = x.size();
    MutableNativeColumnVector state;
    if (args[0].isNull()){
        state.rebind(this->allocateArray<double>(size), size);
    } else {
        MutableNativeColumnVector instate = args[0].getAs<MutableNativeColumnVector>();
        old_state_size = instate.size();
        state.rebind(this->allocateArray<double>(old_state_size + size), 
                     old_state_size + size);

        for (; i < old_state_size; i++) {
            state[i] = instate[i];
        }
    }

    for (int j=0; j < size; j++) {
        state[i+j] = x[j];
    }

    return state;

}} // deep_learning

} // modules

} // madlib
