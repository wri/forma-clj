namespace java forma.generated

struct FireTuple {
  1: i32 temp330;
  2: i32 conf50;
  3: i32 bothPreds;
  4: i32 count;
}

struct FormaValue {
  1: FireTuple fireValue;
  2: double shortDrop;
  3: double longDrop;
  4: double tStat;
  5: double paramBreak;
}

struct NeighborValue {
  1: FireTuple fireValue;
  2: i32 numNeighbors;
  3: double avgShortDrop;
  4: double minShortDrop;
  5: double avgLongDrop;
  6: double minLongDrop;
  7: double avgTStat;
  8: double minTStat;
  9: double avgParamBreak;
  10: double minParamBreak;
}

# Collection Wrappers

struct DoubleArray {
  1: list<double> doubles
}

struct IntArray {
  1: list<i32> ints
}

struct FireSeries {
  1: i32 startIdx
  2: i32 endIdx;
  3: list<FireTuple> values
}

# TODO: Can we add this to DataValue directly?
union ArrayValue {
  1: IntArray ints;
  2: DoubleArray doubles;
}

struct TimeSeries {
  1: i32 startIdx
  2: i32 endIdx;
  3: ArrayValue series;
}

union DataValue {
  1: i32 intVal;
  2: IntArray ints;
  3: double doubleVal;
  4: DoubleArray doubles;
  5: FireTuple fireVal;
  6: TimeSeries timeSeries;
  7: FireSeries fireSeries;
}
