# What is forma-clj? #

The forma-clj project is a [FORMA](http://www.cgdev.org/section/initiatives/_active/forestmonitoringforactionforma) implementation written in the open-source [Clojure](http://clojure.org) programming language. 

At the heart of this implementation is [Cascalog](https://github.com/nathanmarz/cascalog), a fully-featured data processing and querying library for Clojure. It lets us process FORMA data in a reliable, scalable, and distributed way using [MapReduce](http://en.wikipedia.org/wiki/MapReduce) badassery courtesy of [Hadoop](http://hadoop.apache.org). And without the hassle!

Head to the [Project Wiki](https://github.com/sritchie/forma-clj/wiki) for more details.

# Background #

FORMA stands for Forest Monitoring for Action (FORMA) and it uses freely available satellite data to generate rapidly updated online maps of tropical forest clearing, providing useful information for local and national forest conservation programs, as well as international efforts to curb greenhouse gas emissions by paying to keep forests intact.

FORMA was originally a project of the Center for Global Development, an economics think tank in Washington, DC. It is now part of World Resources Institute's Global Forest Watch. WRI is an environmental think tank based in Washington, DC.

# Let's get started #

To get started, you'll need to install a few tools, but it's painless.

* forma-clj (this project)
* Leiningen (Build tool for clojure, located [on github](https://github.com/technomancy/leiningen))
* GDAL (translator and processing library for working with geospatial data formats)
* Plugins

## forma-clj

Fire up your command line and:


```bash
    git clone https://github.com/sritchie/forma-clj.git
    cd forma-clj
```

## Leiningen

Next install Leiningen, the build tool for Clojure. These instructions are copied from the Leiningen README:

* [Download this script](https://raw.github.com/technomancy/leiningen/stable/bin/lein) which is named `lein`
* Place it on your path so that you can execute it. (I like to use `~/bin`)
* Set it to be executable. (`chmod 755 ~/bin/lein`)

## GDAL

OK, so `forma-clj` requires GDAL 1.8.0's native java bindings. GDAL (pronounced "guhdal") is a translator and processing library for working with geospatial data formats. The native bindings can be a bit of a pain to acquire, but they must be built for the system you plan on using. 

If you're using Linux though, we made it easy! 

1. [Download](https://github.com/downloads/sritchie/forma-clj/linuxnative.tar.gz) the native bindings
2. Decompress them into a directory like `/opt/linuxnative`
3. `export LD_LIBRARY_PATH=/opt/linuxnative`

## Plugins

Finally, install the plugins using the `lein` command. This part's easy!

```bash
lein plugin install swank-clojure "1.4.0-SNAPSHOT"
lein plugin install lein-marginalia "0.6.1"
lein plugin install lein-midje "1.0.7"
```

And then, just run `lein deps` to download the dependencies, and run `lein deps` a second time to install them.

And you are DONE. As a sanity check, try compiling via `lein compile`.


# Deploying

See the [forma-deploy](https://github.com/sritchie/forma-deploy) project.

# Issues and Tasks

For project task management, use the [Pivotal Tracker](https://www.pivotaltracker.com/projects/185565).

# Other

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