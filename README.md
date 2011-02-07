===================
FORMA meets Hadoop.
===================

cg

Introduction
============

FORMA's making it happen in 2011. Clojure, Cascalog, Hadoop... What the hell? It's cool. I'll tell you.

Installation
============

Leiningen and Clojure
---------------------

The first thing you'll need to run forma-hadoop is [leiningen](https://github.com/technomancy/leiningen), a self-styled "build tool for Clojure designed to not set your hair on fire". Download the leiningen script from 
[here](https://github.com/technomancy/leiningen/raw/stable/bin/lein) (right click and hit "Save as"), save it into ~/bin, and run the following commands at the terminal::

	$ cd ~/bin
	$ chmod +x lein
	$ lein help

And that's it. (If that last step failed, ~/bin probably isn't on your path. Run::

	$ echo $PATH
	
and put the script in one of those directories.)

GDAL Java bindings
------------------

The attached bindings were built for GDAL 1.8.0; specifically, the Macports hdf4 variant. To install GDAL properly, download the [MacPorts](http://www.macports.org/install.php) DMG for Snow Leopard, and install away.

once this is done, run the following commands at the terminal::

	$ sudo port -v selfupdate
	$ sudo port install gdal +hdf4
	
This will install GDAL 1.8.0 into /opt/local. It won't disturb any other version; we just need it to be in this particular directory because the native java bindings will be looking there.

Finally, ask Sam for the Java native bindings, and put them into::

	/Library/Java/Extensions
	
That'll get you set up to run code within the project.

TextMate Support for Clojure
----------------------------

Check out a great setup for Textmate and Clojure[here](https://github.com/swannodette/textmate-clojure).

Emacs and Aquamacs
------------------

For the adventurous!

Getting Started
===============

Some stuff on how to actually use this damned project, tips on Clojure development.

REPL and Visor
--------------


[Visor](http://visor.binaryage.com/) is totally awesome. Download it!
