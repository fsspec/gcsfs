Changelog
=========

2021.07.0
---------

* fix find/glob with a prefix (#399)

2021.06.1
---------

* kwargs to aiohttpClient session
* graceful timeout when disconnecting at finalise (#397)

2021.06.0
---------

* negative ranges in cat_file (#394)

2021.05.0
---------

* no credentials bug fix (#390)
* use googleapis.com (#388)
* more retries (#387, 385, 380)
* Code cleanup (#381)
* license to match stated one (#378)
* deps updated (#376)

Version 2021.04.0
-----------------

* switch to calver and fsspec pin

Version 0.8.0
-------------

* keep up with fsspec 0.9.0 async
* one-shot find
* consistency checkers
* retries for intermittent issues
* timeouts
* partial cat
* http error status
* CI to GHA

Version 0.7.0
-------------

* async operations via aiohttp


Version 0.6.0
-------------

* **API-breaking**: Changed requester-pays handling for ``GCSFileSystem``.

  The ``user_project`` keyword has been removed, and has been replaced with
  the ``requester_pays`` keyword. If you're working with a ``requester_pays`` bucket
  you will need to explicity pass ``requester_pays-True``. This will include your
  ``project`` ID in requests made to GCS.

Version 0.5.3
-------------

* ``GCSFileSystem`` now validates that the ``project`` provided, if any, matches the
  Google default project when using ``token-'google_default'`` to authenticate (:pr:`219`).
* Fixed bug in ``GCSFileSystem.cat`` on objects in requester-pays buckets (:pr:`217`).

Version 0.5.2
-------------

* Fixed bug in ``user_project`` fallback for default Google authentication (:pr:`213`)

Version 0.5.1
-------------

* ``user_project`` now falls back to the ``project`` if provided (:pr:`208`)

Version 0.5.0
-------------

* Added the ability to make requester-pays requests with the ``user_project`` parameter (:pr:`206`)

Version 0.4.0
-------------

* Improved performance when serializing filesystem objects (:pr:`182`)
* Fixed authorization errors when using ``gcsfs`` within multithreaded code (:pr:`183`, :pr:`192`)
* Added contributing instructions (:pr:`185`)
* Improved performance for :meth:`gcsfs.GCSFileSystem.info` (:pr:`187`)
* Fixed bug in :meth:`gcsfs.GCSFileSystem.info` raising an error (:pr:`190`)
