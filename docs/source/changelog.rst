Changelog
=========

Version 0.5.2
=============

* Fixed bug in ``user_project`` fallback for default Google authentication (:pr:`213`)

Version 0.5.1
=============

* ``user_project`` now falls back to the ``project`` if provided (:pr:`208`)

Version 0.5.0
=============

* Added the ability to make requester-pays requests with the ``user_project`` parameter (:pr:`206`)

Version 0.4.0
=============

* Improved performance when serializing filesystem objects (:pr:`182`)
* Fixed authorization errors when using ``gcsfs`` within multithreaded code (:pr:`183`, :pr:`192`)
* Added contributing instructions (:pr:`185`)
* Improved performance for :meth:`gcsfs.GCSFileSystem.info` (:pr:`187`)
* Fixed bug in :meth:`gcsfs.GCSFileSystem.info` raising an error (:pr:`190`)
