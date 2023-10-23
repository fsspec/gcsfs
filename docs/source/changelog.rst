Changelog
=========

2023.10.0
---------

* Fix for transactions of small files (#586)

2023.9.2
--------

* CI updates (#582)

2023.9.1
--------

* small fixes following #573 (#578)

2023.9.0
--------

* bulk operations edge cases (#576, 572)
* inventory report based file listing (#573)
* pickle HttpError (#571)
* avoid warnings (#569)
* maxdepth in find() (#566)
* invalidate dircache (#564)
* standard metadata field names (#563)
* performance of building cache in find() (#561)


2023.6.0
--------

* allow raw/session token for auth (#554)
* fix listings_expiry_time kwargs (#551)
* allow setting fixed metadata on put/pipe (#550)

2023.5.0
--------

* Allow emulator host without protocol (#548)
* Prevent upload retry from closing the file being sent (#540)

2023.4.0
--------

No changes

2023.3.0
--------

* Don't let find() mess up dircache (#531)
* Drop py3.7 (#529)
* Update docs (#528)
* Make times UTC (#527)
* Use BytesIO for large bodies (#525)
* Fix: Don't append generation when it is absent (#523)
* get/put/cp consistency tests (#521)

2023.1.0
--------

* Support create time (#516, 518)
* defer async session creation (#513, 514)
* support listing of file versions (#509)
* fix ``sign`` following versioned split protocol (#513)

2022.11.0
---------

* implement object versioning (#504)

2022.10.0
---------

* bump fsspec to 2022.10.0 (#503)

2022.8.1
--------

* don't install prerelease aiohttp (#490)

2022.7.1
--------

* Try cloud auth by default (#479)

2022.5.0
--------

* invalidate listings cache for simple put/pipe (#474)
* conform _mkdir and _cat_file to upstream (#471)

2022.3.0
--------

(note that this release happened in 2022.4, but we label as 2022.3 to match
fsspec)

* bucket exists workaround (#464)
* dirmarkers (#459)
* check connection (#457)
* browser connection now uses local server (#456)
* bucket location (#455)
* ensure auth is closed (#452)

2022.02.0
---------

* fix list_buckets without cache (#449)
* drop py36 (#445)

2022.01.0
---------

* update refname for versions (#442)

2021.11.1
---------

* don't touch cache when doing find with a prefix (#437)

2021.11.0
---------

* move to fsspec org
* add support for google fixed_key_metadata (#429)
* deprecate `content_encoding` parameter of setxattrs method (#429)
* use emulator for resting instead of vcrpy (#424)

2021.10.1
---------

* url signing (#411)
* default callback (#422)

2021.10.0
---------

* min version for decorator
* default callback in get (#422)

2021.09.0
---------

* correctly recognise 404 (#419)
* fix for .details due to upstream (#417)
* callbacks in get/put (#416)
* "%" in paths (#415)

2021.08.1
---------

* don't retry 404s (#406)

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
  you will need to explicitly pass ``requester_pays-True``. This will include your
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
