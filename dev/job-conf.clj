(require '[clojure.string :as s])

{"fs.s3n.awsAccessKeyId"     (System/getenv "AWS_KEY")
 "fs.s3n.awsSecretAccessKey" (System/getenv "AWS_SECRET")
 "cascading.kryo.hierarchy.registrations"
 "clojure.lang.IRecord,carbonite.PrintDupSerializer"
 "cascading.kryo.serializations"
 (s/join ":" ["forma.schema.TimeSeriesValue,carbonite.PrintDupSerializer"
              "forma.schema.FireValue,carbonite.PrintDupSerializer"
              "forma.schema.FormaValue,carbonite.PrintDupSerializer"
              "forma.schema.NeighborValue,carbonite.PrintDupSerializer"])}
