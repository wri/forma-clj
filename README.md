Introduction: FORMA meets Hadoop.
=================================

FORMA's making it happen in 2011. Clojure, Cascalog, Hadoop... What the hell? It's cool. I'll tell you.

Installation
============

Cake and Clojure
---------------------

The first thing you'll need to run forma-hadoop is [cake](https://github.com/ninjudd/cake), a self-styled "tasty build tool for Clojure". Follow the instructions at the github page to get started.

GDAL Java bindings
------------------

The attached bindings were built for GDAL 1.8.0; specifically, the Macports hdf4 universal variant. To install GDAL properly, download the [MacPorts](http://www.macports.org/install.php) DMG for Snow Leopard, and install away.

once this is done, run the following commands at the terminal::

	$ sudo port -v selfupdate
	$ sudo port install gdal +hdf4 +universal.
	
This will install GDAL 1.8.0 into /opt/local. It won't disturb any other version of gdal; we just need it to be in this particular directory because the native java bindings will be looking there.

TextMate Support for Clojure
----------------------------

Check out a great setup for Textmate and Clojure [here](https://github.com/swannodette/textmate-clojure). It makes excellent use of cake.

(Also, go into Preferences, Advanced, Shell Variables, and set TM_SOFT_TABS to YES. This will keep us using spaces, not tabs.)

Emacs and Aquamacs
------------------

For the adventurous!

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
