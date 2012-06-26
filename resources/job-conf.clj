(require '[clojure.string :as s])

{"cascading.kryo.hierarchy.registrations"
 "clojure.lang.IRecord,carbonite.PrintDupSerializer"
 "cascading.kryo.serializations"
 (s/join ":" ["forma.thrift.TimeSeriesValue,carbonite.PrintDupSerializer"
              "forma.thrift.FireValue,carbonite.PrintDupSerializer"
              "forma.thrift.FormaValue,carbonite.PrintDupSerializer"
              "forma.thrift.NeighborValue,carbonite.PrintDupSerializer"])}



