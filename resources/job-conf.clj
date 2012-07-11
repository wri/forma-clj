(require '[clojure.string :as s])

{"cascading.kryo.hierarchy.registrations"
 "clojure.lang.IRecord,carbonite.PrintDupSerializer"
 "cascading.kryo.serializations"
 (s/join ":" ["forma.schema.TimeSeriesValue,carbonite.PrintDupSerializer"
              "forma.schema.FireValue,carbonite.PrintDupSerializer"
              "forma.schema.FormaValue,carbonite.PrintDupSerializer"
              "forma.schema.NeighborValue,carbonite.PrintDupSerializer"])}



