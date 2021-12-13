/* ---------------------------------------------------------------
Práctica 3.
Código fuente: Map.h
Grau Informàtica
X5707036T Robert Dragos Trif Apoltan
49271860T Enrique Alejo Subías Melgar
--------------------------------------------------------------- */

#ifndef MAP_H_
#define MAP_H_

#include "Types.h"

#include <string>
#include "MyQueue.h"
#include <map>

using namespace std;

typedef long int TMapInputKey, *PtrMapInputKey;
typedef string TMapInputValue, *PtrMapInputValue;

class MapInputTuple
{
	TMapInputKey Key;
	TMapInputValue Value;

public:
	MapInputTuple(TMapInputKey key, TMapInputValue value) : Key(key), Value(value) {}

	inline TMapInputKey getKey() { return (Key); };
	inline TMapInputValue getValue() { return (Value); };
};
typedef class MapInputTuple TMapInputTuple, *PtrMapInputTuple;

typedef string TMapOutputKey, *PtrMapOutputKey;
typedef int TMapOutputValue, *PtrMapOutputValue;

typedef pair<TMapOutputKey, TMapOutputValue> TMapOuptTuple;

class Map
{
	TError (*MapFunction)(class Map *, TMapInputTuple);

	MyQueue<PtrMapInputTuple> Input;
	multimap<string, int> Output;

public:
	Map(TError (*mapFunction)(class Map *, TMapInputTuple)) : MapFunction(mapFunction){};

	inline multimap<TMapOutputKey, TMapOutputValue> getOutput() { return (Output); };

	TError ReadFileTuples(char *file, struct statistics_split *est_split); //, int start_line, int end_line);
	TError Run(struct statistics_map *est_map);
	void EmitResult(TMapOutputKey key, TMapOutputValue value);

private:
	void AddInput(PtrMapInputTuple tuple, struct statistics_split *est_split);
};
typedef class Map TMap, *PtrMap;

typedef multimap<TMapOutputKey, TMapOutputValue>::const_iterator TMapOuputIterator;

typedef TError (*TMapFunction)(class Map *, TMapInputTuple);

#endif /* MAP_H_ */
