namespace java forma.schema

struct FireTuple {
  1: double maxTemp,
  2: i32 temp330,
  3: i32 conf50,
  4: i32 bothPreds,
  5: i32 count
}

struct TimeSeries {
  1: i32 startPeriod,
  2: i32 endPeriod,
  3: list<FireTuple> values
}
