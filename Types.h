/* ---------------------------------------------------------------
Práctica 3.
Código fuente: Types.h
Grau Informàtica
X5707036T Robert Dragos Trif Apoltan
49271860T Enrique Alejo Subías Melgar
--------------------------------------------------------------- */

#ifndef TYPES_H_
#define TYPES_H_

#include <string>

using namespace std;

#define debug 0

typedef enum
{
    COk,
    CError,
    CErrorOpenInputDir,
    CErrorOpenInputFile,
    CErrorOpenOutputFile
} TError;

void error(string message);

#endif /* TYPES_H_ */
