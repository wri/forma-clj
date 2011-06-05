Introduction: FORMA meets Hadoop.
=================================

FORMA's making it happen in 2011. Clojure, Cascalog, Hadoop... What the hell? It's cool. I'll tell you.

Installation
============

Cake and Clojure
---------------------

The first thing you'll need to run forma-hadoop is
[cake](https://github.com/ninjudd/cake), a self-styled "tasty build
tool for Clojure". Follow the instructions at the github page to get
started.

GDAL Java bindings
------------------

The attached bindings were built for GDAL 1.8.0; specifically, the
Macports hdf4 universal variant. To install GDAL properly, download
the [MacPorts](http://www.macports.org/install.php) DMG for Snow
Leopard, and install away.

once this is done, run the following commands at the terminal::

	$ sudo port -v selfupdate
	$ sudo port install gdal +hdf4 +universal.
	
This will install GDAL 1.8.0 into /opt/local. It won't disturb any
other version of gdal; we just need it to be in this particular
directory because the native java bindings will be looking there.

Emacs 
-----

To get started on OS X, as I believe we all are, go ahead and
[download emacs](http://emacsformacosx.com/). Before opening it up,
delete `~/.emacs` and `~/.emacs.d/`, then run

    git clone https://github.com/whizbangsystems/emacs-starter-kit ~/.emacs.d

to get yourself a copy of the Whizbang emacs starter kit, tweaked for
Clojure.

Once that's done, open up emacs, and run `M-x
package-list-packages`. (The whizbang kit kinds the meta key to
`Command` on the mac.) Install the following packages by hitting `i`
next to them. When you're done, hit `x` to install everything. If you
get failures, please update the documentation to describe what you did
to get around them! Once this is done, quit emacs with `C-c C-z`, and
reboot for utter happiness.

*  browse-kill-ring  1.3.1       installed  Interactively insert items from kill-ring
*  clojure-mode      1.8.1       installed  Major mode for Clojure code
*  ecb               2.40        installed  Emacs Code Browser
*  find-file-in-project 2.1      installed  Find files in a project quickly.
*  gist              0.5         installed  Emacs integration for gist.github.com
*  idle-highlight    1.0         installed  Highlight the word the point is on
*  inf-ruby          2.1         installed  Run a ruby process in a buffer
*  magit             1.0.0       installed  Control Git from Emacs.
*  org               20110421    installed  Outline-based notes management and organizer
*  ruby-mode         1.1         installed  Major mode for editing Ruby files
*  slime             20100404.1  installed  Superior Lisp Interaction Mode for Emacs
*  slime-repl        20100404    installed  Read-Eval-Print Loop written in Emacs Lisp
*  swank-clojure     1.1.0       installed  Slime adapter for clojure
*  textmate          1           installed  TextMate minor mode for Emacs
*  textmate-to-yas   0.13        installed  Import Textmate macros into yasnippet syntax
*  yaml-mode         0.0.5       installed  Major mode for editing YAML files
*  yasnippet-bundle  0.6.1       installed  Yet another snippet extension (Auto compiled bundle)

TextMate Support for Clojure (for Emacs wimps)
----------------------------

Check out a great setup for Textmate and Clojure [here](https://github.com/swannodette/textmate-clojure). It makes excellent use of cake.

(Also, go into Preferences, Advanced, Shell Variables, and set TM_SOFT_TABS to YES. This will keep us using spaces, not tabs.)


Getting Started
===============

I recommend trying out [labrepl](git://github.com/relevance/labrepl.git). To get this done, make sure to remove the "autodoc" line from the dev dependencies!

Some other stuff on how to actually use this damned project, tips on Clojure development.

REPL and Visor
--------------

[Visor](http://visor.binaryage.com/) is totally awesome. Download it!

More Notes
----------

Okay, guys, really cool stuff here. I've been looking at some of the backtype stuff, and they're doing some really interesting work with their data serialization.

1- look at ElephantBird -- piece of code that twitter sent out, that will help us generate taps based on different thrift schema

2- We're going to need to use Thrift to serialize our data, to make sure that it's useful, between all sorts of different languages, etc, and to get to take advantage of some of this more wild stuff that we've seen in the [Nathan Marz presentation at linkedin](http://sna-projects.com/blog/2010/11/clojure-at-backtype/).

* We'll need to define thrift struct -- then, it'd be great to figure out what's going on, with this tap generator. TODO -- also, look at my tap code, and see if I can abstract it out at all.
* Once we create the whole thrift bullshit, I think we need to wrap the new struct inside of a BytesWritable.

### Headers ###

Steps for building the gdal tar file -- 
* tar cf gdal-1.8.0-native-linux-x86_64.tar ./LinuxNative
* gzip -9 gdal-1.8.0-native-linux-x86_64.tar 
* upload to the `reddconfig` bucket on s3

### Other notes

Okay, we built all the gdal stuff... we're going to have to store it
somewhere nice, maybe on S3 or on Dropbox. 

### How to run FORMA ###

Here's the code I used to run fires. Do this inside of `forma.source.fire`:

    (defn run-fires
      [daily-path monthly-path]
      (?- (forma.hadoop.io/chunk-tap "s3n://redddata/" "%s/%s-%s/")
          (->> (union (fire-source-daily (hfs-textline monthly-path))
                      (fire-source-monthly (hfs-textline daily-path)))
               (reproject-fires "1000"))))
    
    ;; (run-fires "/path/to/FIREDAYS/"
    ;;            "/path/to/FIREMONTHS/")

The months business only needed to happen the first time, really. The days are the files that need to get continually updated... This is a bad model, as we can't append onto our datastore. Really, we need to use something like Pail, to give ourselves safe updates.
