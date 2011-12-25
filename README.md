Introduction: FORMA meets Hadoop.
=================================

FORMA's making it happen in 2011. Clojure, Cascalog, Hadoop... What the hell? Head to the [Project Wiki](https://github.com/sritchie/forma-clj/wiki) for more details.

## Setting Up

```bash
lein plugin install swank-clojure "1.4.0-SNAPSHOT"
lein plugin install lein-marginalia "0.6.1"
lein plugin install lein-midje "1.0.7"
```

## Deploying

See the [forma-deploy](https://github.com/sritchie/forma-deploy) project.

## Issues and Tasks

For project task management, use the [Pivotal Tracker](https://www.pivotaltracker.com/projects/185565).

## Other

;; TODO: Run the hansen and vcf special dataset stuff, for diff
;; between big-set and little-set.
;;
;; TODO: Run the ecoid special dataset stuff.
;;
;; TODO: Re-run all timeseries -- might have to jack up the open file
;; limit.
;;
;; TODO: Re-run forma for more countries!

;; :BGD :LAO :IDN :IND :MMR :MYS :PHL :THA :VNM :BOL :CHN :CIV

;; hadoop jar /home/danhammer/forma.jar
;; forma.hadoop.jobs.preprocess.PreprocessAscii "border"
;; /user/hadoop/border.txt s3n://pailbucket/rawstore/ "[11 10]" "[12
;; 11]" "[11 11]" "[30 7]" "[27 5]" "[28 6]" "[29 7]" "[27 6]" "[28
;; 7]" "[26 6]" "[27 7]" "[24 5]" "[25 6]" "[26 7]" "[24 6]" "[25 7]"
;; "[24 7]" "[25 8]" "[17 8]" "[11 9]" "[12 10]"


# EMR Integration #

Added integration for booting spot emr clusters, based on our usual configurations. I think these will work with gdal as well. This is nice, as it'll give us cluster compute support, and bump the number of machines we can use way up.

# Line Counting

```bash
# This needs Homebrew: http://mxcl.github.com/homebrew/
brew install cloc

# Source Lines of Code:
cloc src/ --force-lang="lisp",clj

# Test Lines of Code
cloc test/ --force-lang="lisp",clj
```
