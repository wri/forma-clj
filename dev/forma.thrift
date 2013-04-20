namespace java forma.schema

struct FireValue {
  1: i32 temp330;
  2: i32 conf50;
  3: i32 bothPreds;
  4: i32 count;
}

struct FormaValue {
  1: FireValue fireValue;
  2: double shortDrop;
  3: double longDrop;
  4: double tStat;
  5: double paramBreak;
}

struct NeighborValue {
  1: FireValue fireValue;
  2: i32 numNeighbors;
  3: double avgShortDrop;
  4: double minShortDrop;
  5: double avgLongDrop;
  6: double minLongDrop;
  7: double avgTStat;
  8: double minTStat;
  9: optional double avgParamBreak;
  10: optional double minParamBreak;
}

# Collection Wrappers

struct ShortArray {
  1: list<i16> shorts
}

struct IntArray {
  1: list<i32> ints
}

struct LongArray {
  1: list<i64> longs
}

struct DoubleArray {
  1: list<double> doubles
}

struct FireArray {
  1: list<FireValue> fires;
}

struct FormaArray {
  1: list<FormaValue> values;
}

union ArrayValue {
  1: LongArray longs;
  2: DoubleArray doubles;
  3: ShortArray shorts;
  4: IntArray ints;
  5: FireArray fires;
  6: FormaArray formas;
}

struct TimeSeries {
  1: i32 startIdx;
  2: i32 endIdx;
  3: ArrayValue series;
}

union DataValue {
  1: double doubleVal;
  2: i32 intVal;
  3: i64 longVal;
  4: i16 shortVal;
  5: FireValue fireVal;
  6: TimeSeries timeSeries;
  7: ArrayValue vals;
  8: FormaValue forma;
}

struct ModisPixelLocation {
  1: string resolution;
  2: i32 tileH;
  3: i32 tileV;
  4: i32 sample;
  5: i32 line;
}

struct ModisChunkLocation {
  1: string resolution;
  2: i32 tileH;
  3: i32 tileV;
  4: i32 chunkID;
  5: i32 chunkSize;
}

union LocationPropertyValue {
  1: ModisPixelLocation pixelLocation;
  2: ModisChunkLocation chunkLocation;
}

struct LocationProperty {
  1: LocationPropertyValue property;
}

struct Pedigree {
  1: required i32 trueAsOfSecs;
}

struct DataChunk {
  1: string dataset;
  2: LocationProperty locationProperty;
  3: DataValue chunkValue;
  4: string temporalRes;
  5: optional string date;
  6: optional Pedigree pedigree;
}
