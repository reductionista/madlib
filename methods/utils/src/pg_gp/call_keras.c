#include "Python.h"

int main()
{
//    PyCodeObject code;
//    PyObject globals;
    long rv = -1;

    long b = Py_IsInitialized();
    if (b) {
        printf("Yup, python is aleady initialized.\n");
    } else {
        Py_Initialize();
    }

//    code = PyCode_New(0, 1, 512, 0, code_obj, consts, names, varnames, freevars, cellvars, filename, name, int firstlineno, lnotab);

#if PY_VERSION_HEX >= 0x03020000
//    rv = PyEval_EvalCode(code, globals, globals);
#else
//    rv = PyEval_EvalCode(code, globals, globals);
#endif

    printf("Running: import keras...\n");
    rv = PyRun_SimpleString("import keras");
    printf("Python call returned with error code %ld\n", rv);
    return rv;
}
