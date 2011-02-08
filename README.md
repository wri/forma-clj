Introduction: FORMA meets Hadoop.
=================================

FORMA's making it happen in 2011. Clojure, Cascalog, Hadoop... What the hell? It's cool. I'll tell you.

Installation
============

Cake and Clojure
---------------------

The first thing you'll need to run forma-hadoop is [cake](https://github.com/ninjudd/cake), a self-styled "tasty build tool for Clojure". Follow the instructions at the github page to get it installed!

GDAL Java bindings
------------------

The attached bindings were built for GDAL 1.8.0; specifically, the Macports hdf4 variant. To install GDAL properly, download the [MacPorts](http://www.macports.org/install.php) DMG for Snow Leopard, and install away.

once this is done, run the following commands at the terminal::

	$ sudo port -v selfupdate
	$ sudo port install gdal +hdf4
	
This will install GDAL 1.8.0 into /opt/local. It won't disturb any other version; we just need it to be in this particular directory because the native java bindings will be looking there.

TextMate Support for Clojure
----------------------------

Check out a great setup for Textmate and Clojure [here](https://github.com/swannodette/textmate-clojure). It makes excellent use of cake.

(Also, go into Preferences, Advanced, Shell Variables, and set TM_SOFT_TABS to YES. This will keep us using spaces, not tabs.)

Emacs and Aquamacs
------------------

For the adventurous!

Getting Started
===============

Some stuff on how to actually use this damned project, tips on Clojure development.

REPL and Visor
--------------

[Visor](http://visor.binaryage.com/) is totally awesome. Download it!
