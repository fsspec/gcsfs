# -*- coding: utf-8 -*-
"""
Google Cloud Storage pythonic interface
"""
from __future__ import print_function

import decorator

import array
from base64 import b64encode, b64decode
import google.auth as gauth
import google.auth.compute_engine
from google.auth.transport.requests import AuthorizedSession
from google.auth.exceptions import GoogleAuthError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2 import service_account
from hashlib import md5
import io
import json
import logging
import traceback
import os
import posixpath
import pickle
import re
import requests
import sys
import time
import warnings
import random

from requests.exceptions import RequestException
from .utils import HttpError, RateLimitException, is_retriable, read_block

PY2 = sys.version_info.major == 2

logger = logging.getLogger(__name__)

# Allow optional tracing of call locations for api calls.
# Disabled by default to avoid *massive* test logs.
_TRACE_METHOD_INVOCATIONS = False

@decorator.decorator
def _tracemethod(f, self, *args, **kwargs):
    logger.debug("%s(args=%s, kwargs=%s)", f.__name__, args, kwargs)
    if _TRACE_METHOD_INVOCATIONS and logger.isEnabledFor(logging.DEBUG-1):
        tb_io = io.StringIO()
        traceback.print_stack(file=tb_io)
        logger.log(logging.DEBUG - 1, tb_io.getvalue())

    return f(self, *args, **kwargs)


# client created 2018-01-16
not_secret = {"client_id": "586241054156-8986sjc0h0683jmpb150i0m8cucrttds"
                           ".apps.googleusercontent.com",
              "client_secret": "8_Gk27xMtJzX6tkViMGF2K1B"}
client_config = {'installed': {
    'client_id': not_secret['client_id'],
    'client_secret': not_secret['client_secret'],
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://accounts.google.com/o/oauth2/token"
}}
tfile = os.path.join(os.path.expanduser("~"), '.gcs_tokens')
ACLs = {"authenticatedread", "bucketownerfullcontrol", "bucketownerread",
        "private", "projectprivate", "publicread"}
bACLs = {"authenticatedRead", "private", "projectPrivate", "publicRead",
         "publicReadWrite"}
DEFAULT_PROJECT = os.environ.get('GCSFS_DEFAULT_PROJECT', '')

GCS_MIN_BLOCK_SIZE = 2 ** 18
DEFAULT_BLOCK_SIZE = 5 * 2 ** 20

if PY2:
    FileNotFoundError = IOError


def quote_plus(s):
    """
    Convert some URL elements to be HTTP-safe.

    Not the same as in urllib, because, for instance, parentheses and commas
    are passed through.

    Parameters
    ----------
    s: input URL/portion

    Returns
    -------
    corrected URL
    """
    s = s.replace('/', '%2F')
    s = s.replace(' ', '%20')
    return s


def norm_path(path):
    """Canonicalize path to '{bucket}/{name}' form."""
    return "/".join(split_path(path))

def split_path(path):
    """
    Normalise GCS path string into bucket and key.

    Parameters
    ----------
    path : string
        Input path, like `gcs://mybucket/path/to/file`.
        Path is of the form: '[gs|gcs://]bucket[/key]'

    Returns
    -------
        (bucket, key) tuple

    Examples
    --------
    >>> split_path("gcs://mybucket/path/to/file")
    ['mybucket', 'path/to/file']
    >>> split_path("mybucket/path/to/file")
    ['mybucket', 'path/to/file']
    >>> split_path("gs://mybucket")
    ['mybucket', '']
    """
    if path.startswith('gcs://'):
        path = path[6:]
    if path.startswith('gs://'):
        path = path[5:]
    if path.startswith('/'):
        path = path[1:]
    if '/' not in path:
        return path, ""
    else:
        return path.split('/', 1)


def validate_response(r, path):
    """
    Check the requests object r, raise error if it's not ok.

    Parameters
    ----------
    r: requests response object
    path: associated URL path, for error messages
    """
    if not r.ok:
        m = str(r.content)
        error = None
        try:
            error = r.json()['error']
            msg = error['message']
        except:
            msg = str(r.content)

        if r.status_code == 404:
            raise FileNotFoundError(path)
        elif r.status_code == 403:
            raise IOError("Forbidden: %s\n%s" % (path, msg))
        elif r.status_code == 429:
            raise RateLimitException(error)
        elif "invalid" in m:
            raise ValueError("Bad Request: %s\n%s" % (path, msg))
        elif error:
            raise HttpError(error)
        else:
            raise RuntimeError(m)


class GCSFileSystem(object):
    """
    Connect to Google Cloud Storage.

    The following modes of authentication are supported:

    - ``token=None``, GCSFS will attempt to guess your credentials in the
      following order: gcloud CLI default, gcsfs cached token, google compute
      metadata service, anonymous.
    - ``token='google_default'``, your default gcloud credentials will be used,
      which are typically established by doing ``gcloud login`` in a terminal.
    - ``token=='cache'``, credentials from previously successful gcsfs
      authentication will be used (use this after "browser" auth succeeded)
    - ``token='anon'``, no authentication is preformed, and you can only
      access data which is accessible to allUsers (in this case, the project and
      access level parameters are meaningless)
    - ``token='browser'``, you get an access code with which you can
      authenticate via a specially provided URL
    - if ``token='cloud'``, we assume we are running within google compute
      or google container engine, and query the internal metadata directly for
      a token.
    - you may supply a token generated by the
      [gcloud](https://cloud.google.com/sdk/docs/)
      utility; this is either a python dictionary, the name of a file
      containing the JSON returned by logging in with the gcloud CLI tool,
      or a Credentials object. gcloud typically stores its tokens in locations
      such as
      ``~/.config/gcloud/application_default_credentials.json``,
      `` ~/.config/gcloud/credentials``, or
      ``~\AppData\Roaming\gcloud\credentials``, etc.

    Specific methods, (eg. `ls`, `info`, ...) may return object details from GCS.
    These detailed listings include the 
    [object resource](https://cloud.google.com/storage/docs/json_api/v1/objects#resource)
    with additional properties:

        - "path" : string
            The "{bucket}/{name}" path of the object, used in calls to GCSFileSystem or GCSFile.

    GCS *does not* include  "directory" objects but instead generates directories by splitting
    [object names](https://cloud.google.com/storage/docs/key-terms). This means that, for example,
    a directory does not need to exist for an object to be created within it. Creating an object 
    implicitly creates it's parent directories, and removing all objects from a directory implicitly
    deletes the empty directory.

    `GCSFileSystem` generates listing entries for these implied directories in listing apis with the 
    object properies:

        - "path" : string
            The "{bucket}/{name}" path of the dir, used in calls to GCSFileSystem or GCSFile.
        - "bucket" : string
            The name of the bucket containing this object.
        - "name" : string
            The "/" terminated name of the directory within the bucket.
        - "kind" : 'storage#object'
        - "size" : 0
        - "storageClass" : 'DIRECTORY'
    

    GCSFileSystem maintains a per-implied-directory cache of object listings and fulfills all
    object information and listing requests from cache. This implied, for example, that objects
    created via other processes *will not* be visible to the GCSFileSystem until the cache
    refreshed. Calls to GCSFileSystem.open and calls to GCSFile are not effected by this cache.

    In the default case the cache is never expired. This may be controlled via the `cache_timeout`
    GCSFileSystem parameter or via explicit calls to `GCSFileSystem.invalidate_cache`.

    Parameters
    ----------
    project : string
        project_id to work under. Note that this is not the same as, but ofter
        very similar to, the project name.
        This is required in order
        to list all the buckets you have access to within a project and to
        create/delete buckets, or update their access policies.
        If ``token='google_default'``, the value is overriden by the default,
        if ``token='anon'``, the value is ignored.
    access : one of {'read_only', 'read_write', 'full_control'}
        Full control implies read/write as well as modifying metadata,
        e.g., access control.
    token: None, dict or string
        (see description of authentication methods, above)
    consistency: 'none', 'size', 'md5'
        Check method when writing files. Can be overridden in open().
    cache_timeout: float, seconds
        Cache expiration time in seconds for object metadata cache.
        Set cache_timeout <= 0 for no caching, None for no cache expiration.
    secure_serialize: bool
        If True, instances re-establish auth upon deserialization; if False,
        token is passed directly, which may be a security risk if passed
        across an insecure network.
    check_connection: bool
        When token=None, gcsfs will attempt various methods of establishing
        credentials, falling back to anon. It is possible for a methoc to
        find credentials in the system that turn out not to be valid. Setting
        this parameter to True will ensure that an actual operation is
        attempted before deciding that credentials are valid.
    """
    scopes = {'read_only', 'read_write', 'full_control'}
    retries = 6  # number of retries on http failure
    base = "https://www.googleapis.com/storage/v1/"
    _singleton = [None]
    _singleton_pars = [None]
    default_block_size = DEFAULT_BLOCK_SIZE

    def __init__(self, project=DEFAULT_PROJECT, access='full_control',
                 token=None, block_size=None, consistency='none',
                 cache_timeout=None, secure_serialize=True,
                 check_connection=True):
        pars = (project, access, token, block_size, consistency, cache_timeout)
        if access not in self.scopes:
            raise ValueError('access must be one of {}', self.scopes)
        if project is None:
            warnings.warn('GCS project not set - cannot list or create buckets')
        if block_size is not None:
            self.default_block_size = block_size
        self.project = project
        self.access = access
        self.scope = "https://www.googleapis.com/auth/devstorage." + access
        self.consistency = consistency
        self.token = token
        self.cache_timeout = cache_timeout
        self.check_credentials = check_connection
        if pars == self._singleton_pars[0]:
            inst = self._singleton[0]
            self.session = inst.session
            self._listing_cache = inst._listing_cache
            self.token = inst.token

        else:
            self._listing_cache = {}
            self.session = None
            self.connect(method=token)

        self._singleton[0] = self
        self._singleton_pars[0] = pars
        if not secure_serialize:
            self.token = self.session.credentials


    @classmethod
    def current(cls):
        """ Return the most recently created GCSFileSystem

        If no GCSFileSystem has been created, then create one
        """
        if not cls._singleton[0]:
            return GCSFileSystem()
        else:
            return cls._singleton[0]

    @staticmethod
    def load_tokens():
        """Get "browser" tokens from disc"""
        try:
            with open(tfile, 'rb') as f:
                tokens = pickle.load(f)
            # backwards compatability
            tokens = {k: (GCSFileSystem._dict_to_credentials(v)
                          if isinstance(v, dict) else v)
                      for k, v in tokens.items()}
        except Exception:
            tokens = {}
        GCSFileSystem.tokens = tokens

    def _connect_google_default(self):
        credentials, project = gauth.default(scopes=[self.scope])
        self.project = project
        self.session = AuthorizedSession(credentials)

    def _connect_cloud(self):
        credentials = gauth.compute_engine.Credentials()
        self.session = AuthorizedSession(credentials)

    def _connect_cache(self):
        project, access = self.project, self.access
        if (project, access) in self.tokens:
            credentials = self.tokens[(project, access)]
            self.session = AuthorizedSession(credentials)

    def _dict_to_credentials(self, token):
        """
        Convert old dict-style token.

        Does not preserve access token itself, assumes refresh required.
        """
        try:
            token = service_account.Credentials.from_service_account_info(
                token, scopes=[self.scope])
        except:
            token = Credentials(
                None, refresh_token=token['refresh_token'],
                client_secret=token['client_secret'],
                client_id=token['client_id'],
                token_uri='https://www.googleapis.com/oauth2/v4/token',
                scopes=[self.scope]
            )
        return token

    def _connect_token(self, token):
        """
        Connect using a concrete token

        Parameters
        ----------
        token: str, dict or Credentials
            If a str, try to load as a Service file, or next as a JSON; if
            dict, try to interpret as credentials; if Credentials, use directly.
        """
        if isinstance(token, str):
            if not os.path.exists(token):
                raise FileNotFoundError(token)
            try:
                # is this a "service" token?
                self._connect_service(token)
                return
            except:
                # some other kind of token file
                # will raise exception if is not json
                token = json.load(open(token))
        if isinstance(token, dict):
            credentials = self._dict_to_credentials(token)
        elif isinstance(token, Credentials):
            credentials = token
        else:
            raise ValueError('Token format no understood')
        self.session = AuthorizedSession(credentials)

    def _connect_service(self, fn):
        # raises exception if file does not match expectation
        credentials = service_account.Credentials.from_service_account_file(
            fn, scopes=[self.scope])
        self.session = AuthorizedSession(credentials)

    def _connect_anon(self):
        self.session = requests.Session()

    def _connect_browser(self):
        flow = InstalledAppFlow.from_client_config(client_config, [self.scope])
        credentials = flow.run_console()
        self.tokens[(self.project, self.access)] = credentials
        self._save_tokens()
        self.session = AuthorizedSession(credentials)

    def connect(self, method=None):
        """
        Establish session token. A new token will be requested if the current
        one is within 100s of expiry.

        Parameters
        ----------
        method: str (google_default|cache|cloud|token|anon|browser) or None
            Type of authorisation to implement - calls `_connect_*` methods.
            If None, will try sequence of methods.
        """
        if method not in ['google_default', 'cache', 'cloud', 'token', 'anon',
                          'browser', None]:
            self._connect_token(method)
        elif method is None:
            for meth in ['google_default', 'cache', 'anon']:
                try:
                    self.connect(method=meth)
                    if self.check_credentials and meth != 'anon':
                        self.ls('anaconda-public-data')
                except:
                    self.session = None
                    logger.debug('Connection with method "%s" failed' % meth)
                if self.session:
                    break
        else:
            self.__getattribute__('_connect_' + method)()
            self.method = method
        if self.session is None:
            if method is None:
                msg = ("Automatic authentication failed, you should try "
                       "specifying a method with the token= kwarg")
            else:
                msg = ("Auth failed with method '%s'. See the docstrings for "
                       "further details about your auth mechanism, also "
                       "available at https://gcsfs.readthedocs.io/en/latest/"
                       "api.html#gcsfs.core.GCSFileSystem" % method)
            raise RuntimeError(msg)

    @staticmethod
    def _save_tokens():
        try:
            with open(tfile, 'wb') as f:
                pickle.dump(GCSFileSystem.tokens, f, 2)
        except Exception as e:
            warnings.warn('Saving token cache failed: ' + str(e))

    @_tracemethod
    def _call(self, method, path, *args, **kwargs):
        for k, v in list(kwargs.items()):
            if v is None:
                del kwargs[k]
        json = kwargs.pop('json', None)
        headers = kwargs.pop('headers', None)
        data = kwargs.pop('data', None)

        if not path.startswith('http'):
            path = self.base + path

        if args:
            path = path.format(*[quote_plus(p) for p in args])

        for retry in range(self.retries):
            try:
                if retry > 0:
                    time.sleep(min(random.random() + 2**(retry-1), 32))
                r = self.session.request(method, path,
                                         params=kwargs, json=json, headers=headers, data=data)
                validate_response(r, path)
                break
            except (HttpError, RequestException, RateLimitException, GoogleAuthError) as e:
                if retry == self.retries - 1:
                    logger.exception("_call out of retries on exception: %s", e)
                    raise e
                if is_retriable(e):
                    logger.debug("_call retrying after exception: %s", e)
                    continue
                logger.exception("_call non-retriable exception: %s", e)
                raise e

        return r

    @property
    def buckets(self):
        """Return list of available project buckets."""
        return [b["name"] for b in self._list_buckets()["items"]]

    @classmethod
    def _process_object(self, bucket, object_metadata):
        """Process object resource into gcsfs object information format.
        
        Process GCS object resource via type casting and attribute updates to
        the cache-able gcsf object information format. Returns an updated copy
        of the object resource.

        (See https://cloud.google.com/storage/docs/json_api/v1/objects#resource)
        """
        result = dict(object_metadata)
        result["size"] = int(object_metadata.get("size", 0))
        result["path"] = posixpath.join(bucket, object_metadata["name"])

        return result

    @_tracemethod
    def _get_object(self, path):
        """Return object information at the given path."""
        bucket, key = split_path(path)

        # Check if parent dir is in listing cache
        parent = "/".join([bucket, posixpath.dirname(key.rstrip("/"))]) + "/"
        parent_cache = self._maybe_get_cached_listing(parent)
        if parent_cache:
            cached_obj = [o for o in parent_cache["items"] if o["name"] == key]
            if cached_obj:
                logger.debug("found cached object: %s", cached_obj)
                return cached_obj[0]
            else:
                logger.debug("object not found cached parent listing")
                raise FileNotFoundError(path)

        if not key:
            # Attempt to "get" the bucket root, return error instead of
            # listing.
            raise FileNotFoundError(path)

        result = self._process_object(bucket, self._call('GET', 'b/{}/o/{}', bucket, key).json())

        return result

    @_tracemethod
    def _maybe_get_cached_listing(self, path):
        logger.debug("_maybe_get_cached_listing: %s", path)
        if path in self._listing_cache:
            retrieved_time, listing = self._listing_cache[path]
            cache_age = time.time() - retrieved_time
            if self.cache_timeout is not None and cache_age > self.cache_timeout:
                logger.debug(
                    "expired cache path: %s retrieved_time: %.3f cache_age: "
                    "%.3f cache_timeout: %.3f",
                    path, retrieved_time, cache_age, self.cache_timeout
                )
                del self._listing_cache[path]
                return None

            return listing

        return None

    @_tracemethod
    def _list_objects(self, path):
        path = norm_path(path)

        clisting = self._maybe_get_cached_listing(path)
        if clisting:
            return clisting

        listing = self._do_list_objects(path)
        retrieved_time = time.time()

        self._listing_cache[path] = (retrieved_time, listing)
        return listing

    @_tracemethod
    def _do_list_objects(self, path, max_results=None):
        """Object listing for the given {bucket}/{prefix}/ path."""
        bucket, prefix = split_path(path)
        if not prefix:
            prefix = None

        prefixes = []
        items = []
        page = self._call('GET', 'b/{}/o/', bucket,
                          delimiter="/", prefix=prefix, maxResults=max_results).json()

        assert page["kind"] == "storage#objects"
        prefixes.extend(page.get("prefixes", []))
        items.extend(page.get("items", []))
        next_page_token = page.get('nextPageToken', None)

        while next_page_token is not None:
            page = self._call('GET', 'b/{}/o/', bucket,
                              delimiter="/", prefix=prefix, maxResults=max_results, pageToken=next_page_token).json()

            assert page["kind"] == "storage#objects"
            prefixes.extend(page.get("prefixes", []))
            items.extend(page.get("items", []))
            next_page_token = page.get('nextPageToken', None)

        result = {
            "kind": "storage#objects",
            "prefixes": prefixes,
            "items": [self._process_object(bucket, i) for i in items],
        }

        return result

    @_tracemethod
    def _list_buckets(self):
        """Return list of all buckets under the current project."""
        items = []
        page = self._call('GET', 'b/',
                          project=self.project).json()

        assert page["kind"] == "storage#buckets"
        items.extend(page.get("items", []))
        next_page_token = page.get('nextPageToken', None)

        while next_page_token is not None:
            page = self._call(
                'GET', 'b/', project=self.project, pageToken=next_page_token).json()

            assert page["kind"] == "storage#buckets"
            items.extend(page.get("items", []))
            next_page_token = page.get('nextPageToken', None)

        result = {
            "kind": "storage#buckets",
            "items": items,
        }

        return result

    @_tracemethod
    def invalidate_cache(self, path=None):
        """
        Invalidate listing cache for given path, it is reloaded on next use.

        Parameters
        ----------
        path: string or None
            If None, clear all listings cached else listings at or under given
            path.
        """

        if not path:
            logger.debug("invalidate_cache clearing cache")
            self._listing_cache.clear()
        else:
            path = norm_path(path)

            invalid_keys = [k for k in self._listing_cache
                            if k.startswith(path)]

            for k in invalid_keys:
                self._listing_cache.pop(k, None)

    @_tracemethod
    def mkdir(self, bucket, acl='projectPrivate',
              default_acl='bucketOwnerFullControl'):
        """
        New bucket

        Parameters
        ----------
        bucket: str
            bucket name
        acl: string, one of bACLs
            access for the bucket itself
        default_acl: str, one of ACLs
            default ACL for objects created in this bucket
        """
        self._call('POST', 'b/',
                   predefinedAcl=acl, project=self.project, predefinedDefaultObjectAcl=default_acl,
                   json={"name": bucket})
        self.invalidate_cache(bucket)

    @_tracemethod
    def rmdir(self, bucket):
        """Delete an empty bucket"""
        self._call('DELETE', 'b/' + bucket)
        self.invalidate_cache(bucket)

    @_tracemethod
    def ls(self, path, detail=False):
        """List objects under the given '/{bucket}/{prefix} path."""
        path = norm_path(path)

        if path in ['/', '']:
            return self.buckets
        elif path.endswith("/"):
            return self._ls(path, detail)
        else:
            combined_listing = self._ls(path, detail) + self._ls(path + "/",
                                                                 detail)
            if detail:
                combined_entries = dict(
                    (l["path"], l) for l in combined_listing)
                combined_entries.pop(path + "/", None)
                return list(combined_entries.values())
            else:
                return list(set(combined_listing) - {path + "/"})

    @_tracemethod
    def _ls(self, path, detail=False):
        listing = self._list_objects(path)
        bucket, key = split_path(path)

        if not detail:

            # Convert item listing into list of 'item' and 'subdir/'
            # entries. Items may be of form "key/", in which case there
            # will be duplicate entries in prefix and item_names.
            item_names = [f["name"] for f in listing["items"] if f["name"]]
            prefixes = [p for p in listing["prefixes"]]

            return [
                posixpath.join(bucket, n) for n in set(item_names + prefixes)
            ]

        else:
            item_details = listing["items"]

            pseudodirs = [{
                    'bucket': bucket,
                    'name': prefix,
                    'path': bucket + "/" + prefix,
                    'kind': 'storage#object',
                    'size': 0,
                    'storageClass': 'DIRECTORY',
                }
                for prefix in listing["prefixes"]
            ]

            return item_details + pseudodirs

    @_tracemethod
    def walk(self, path, detail=False):
        """ Return all real keys belows path. """
        path = norm_path(path)

        if path in ("/", ""):
            raise ValueError("path must include at least target bucket")

        if path.endswith('/'):
            listing = self.ls(path, detail=True)

            files = [l for l in listing if l["storageClass"] != "DIRECTORY"]
            dirs = [l for l in listing if l["storageClass"] == "DIRECTORY"]
            for d in dirs:
                files.extend(self.walk(d["path"], detail=True))
        else:
            files = self.walk(path + "/", detail=True)

            try:
                obj = self.info(path)
                if obj["storageClass"] != "DIRECTORY":
                    files.append(obj)
            except FileNotFoundError:
                pass

        if detail:
            return files
        else:
            return [f["path"] for f in files]

    @_tracemethod
    def du(self, path, total=False, deep=False):
        """Bytes used by keys at the given path

        Parameters
        ----------
        total: bool
            If True, returns a single integer which is the sum of the file
            sizes; otherwise returns a {key: size} dictionary
        deep: bool
            Whether to descend into child directories
        """
        if deep:
            files = self.walk(path, True)
        else:
            files = [f for f in self.ls(path, True)]
        if total:
            return sum(f['size'] for f in files)
        return {f['path']: f['size'] for f in files}

    @_tracemethod
    def glob(self, path):
        """
        Find files by glob-matching.

        Note that the bucket part of the path must not contain a "*"
        """
        path = path.rstrip('/')
        bucket, key = split_path(path)
        path = '/'.join([bucket, key])
        if "*" in bucket:
            raise ValueError('Bucket cannot contain a "*"')
        if '*' not in path:
            path = path.rstrip('/') + '/*'
        if '/' in path[:path.index('*')]:
            ind = path[:path.index('*')].rindex('/')
            root = path[:ind + 1]
        else:
            root = ''
        allfiles = self.walk(root)
        pattern = re.compile("^" + path.replace('//', '/')
                             .rstrip('/').replace('**', '.+')
                             .replace('*', '[^/]+')
                             .replace('?', '.') + "$")
        out = [f for f in allfiles if re.match(pattern,
               f.replace('//', '/').rstrip('/'))]
        return out

    @_tracemethod
    def exists(self, path):
        """Is there a key at the given path?"""
        bucket, key = split_path(path)
        try:
            if key:
                return bool(self.info(path))
            else:
                if bucket in self.buckets:
                    return True
                else:
                    try:
                        # Bucket may be present & viewable, but not owned by
                        # the current project. Attempt to list.
                        self._list_objects(path)
                        return True
                    except (FileNotFoundError, IOError, ValueError):
                        # bucket listing failed as it doesn't exist or we can't
                        # see it
                        return False
        except FileNotFoundError:
            return False

    @_tracemethod
    def info(self, path):
        """Get information about specific key

        Returns a dictionary with the full name, size and type of the key.
        The path will be labeled as directory-type if it doesn't exist as a
        key, but there are sub-keys to the given path
        """
        bucket, key = split_path(path)
        if not key:
            # Return a pseudo dir for the bucket root
            # TODO: check that it exists (either is in bucket list,
            # or can list it)
            return {
                'bucket': bucket,
                'name': "/",
                'path': bucket + "/",
                'kind': 'storage#object',
                'size': 0,
                'storageClass': 'DIRECTORY',
            }

        try:
            return self._get_object(path)
        except FileNotFoundError:
            logger.debug("info FileNotFound at path: %s", path)
            # ls containing directory of path to determine
            # if a pseudodirectory is needed for this entry.
            ikey = key.rstrip("/")
            dkey = ikey + "/"
            assert ikey, "Stripped path resulted in root object."

            parent_listing = self.ls(
                posixpath.join(bucket, posixpath.dirname(ikey)), detail=True)
            pseudo_listing = [
                i for i in parent_listing
                if i["storageClass"] == "DIRECTORY" and i["name"] == dkey ]

            if pseudo_listing:
                return pseudo_listing[0]
            else:
                raise

    @_tracemethod
    def url(self, path):
        """ Get HTTP URL of the given path from info entry """
        # TODO: could be implemented without info call, see cat()
        return self.info(path)['mediaLink']

    @_tracemethod
    def cat(self, path):
        """ Simple one-shot get of file data """
        u = 'https://www.googleapis.com/download/storage/v1/b/{}/o/{}?alt=media'
        bucket, object = split_path(path)
        object = quote_plus(object)
        u2 = u.format(bucket, object)
        r = self.session.get(u2)
        r.raise_for_status()
        if 'X-Goog-Hash' in r.headers:
            # if header includes md5 hash, check that data matches
            bits = r.headers['X-Goog-Hash'].split(',')
            for bit in bits:
                key, val = bit.split('=', 1)
                if key == 'md5':
                    md = b64decode(val)
                    assert md5(r.content).digest() == md, "Checksum failure"
        return r.content

    @_tracemethod
    def get(self, rpath, lpath, blocksize=5 * 2 ** 20, recursive=False):
        """Download remote files to local

        Parameters
        ----------
        rpath: str
            Remote location
        lpath: str
            Local location
        blocksize: int
            Chunks in which the data is fetched
        recursive: bool
            If true, recursively download files in subdirectories.
        """
        if recursive:
            rpaths = self.walk(rpath)
            rootdir = os.path.basename(rpath.rstrip('/'))
            if os.path.isdir(lpath):
                # copy rpath inside lpath directory
                lpath2 = os.path.join(lpath, rootdir)
            else:
                # copy rpath as lpath directory
                lpath2 = lpath
            lpaths = [os.path.join(lpath2, path[len(rpath):].lstrip('/')) for path in rpaths]
            for lpath in lpaths:
                dirname = os.path.dirname(lpath)
                if not os.path.isdir(dirname):
                    os.makedirs(dirname)
        else:
            rpaths = [rpath]
            lpaths = [lpath]
        for rpath, lpath in zip(rpaths, lpaths):
            with self.open(rpath, 'rb', block_size=blocksize) as f1:
                with open(lpath, 'wb') as f2:
                    while True:
                        d = f1.read(blocksize)
                        if not d:
                            break
                        f2.write(d)

    @_tracemethod
    def put(self, lpath, rpath, blocksize=5 * 2 ** 20, acl=None,
            metadata=None, recursive=False):
        """Upload local files to remote

        Parameters
        ----------
        lpath: str
            Local location
        rpath: str
            Remote location
        blocksize: int
            Chunks in which the data is sent
        acl: str or None
            Optional access control to apply to the created object
        metadata: None or dict
            Gets added to object metadata on server
        recursive: bool
            If true, recursively upload files in subdirectories
        """
        if recursive:
            lpaths = []
            for dirname, subdirlist, filelist in os.walk(lpath):
                lpaths += [os.path.join(dirname, filename) for filename in filelist]
            rootdir = os.path.basename(lpath.rstrip('/'))
            if self.exists(rpath):
                # copy lpath inside rpath directory
                rpath2 = os.path.join(rpath, rootdir)
            else:
                # copy lpath as rpath directory
                rpath2 = rpath
            rpaths = [os.path.join(rpath2, path[len(lpath):].lstrip('/')) for path in lpaths]
        else:
            lpaths = [lpath]
            rpaths = [rpath]
        for lpath, rpath in zip(lpaths, rpaths):
            with self.open(rpath, 'wb', block_size=blocksize, acl=acl,
                           metadata=metadata) as f1:
                with open(lpath, 'rb') as f2:
                    while True:
                        d = f2.read(blocksize)
                        if not d:
                            break
                        f1.write(d)

    def getxattr(self, path, attr):
        """Get user-defined metadata attribute"""
        meta = self.info(path).get('metadata', {})
        return meta[attr]

    def setxattrs(self, path, content_type=None, content_encoding=None, **kwargs):
        """ Set/delete/add writable metadata attributes

        Parameters
        ---------
        content_type: str
            If not None, set the content-type to this value
        content_encoding: str
            If not None, set the content-encoding. See https://cloud.google.com/storage/docs/transcoding
        kw_args: key-value pairs like field="value" or field=None
            value must be string to add or modify, or None to delete

        Returns
        -------
        Entire metadata after update (even if only path is passed)
        """
        i_json = {'metadata': kwargs}
        if content_type is not None:
            i_json['contentType'] = content_type
        if content_encoding is not None:
            i_json['contentEncoding'] = content_encoding

        bucket, key = split_path(path)
        o_json = self._call('PATCH', "b/{}/o/{}", bucket, key,
                            fields='metadata', json=i_json)\
            .json()
        return o_json.get('metadata', {})

    @_tracemethod
    def head(self, path, size=1024):
        """ Fetch start of file

        Parameters
        ----------
        path: str
            File location
        size: int
            Number of bytes to fetch

        Returns
        -------
        Up to "size" number of bytes
        """
        with self.open(path, 'rb') as f:
            return f.read(size)

    @_tracemethod
    def tail(self, path, size=1024):
        """ Fetch end of file

        Parameters
        ----------
        path: str
            File location
        size: int
            Number of bytes to fetch

        Returns
        -------
        Up to "size" number of bytes
        """
        if size > self.info(path)['size']:
            return self.cat(path)
        with self.open(path, 'rb') as f:
            f.seek(-size, 2)
            return f.read()

    @_tracemethod
    def merge(self, path, paths, acl=None):
        """Concatenate objects within a single bucket"""
        bucket, key = split_path(path)
        source = [{'name': split_path(p)[1]} for p in paths]
        self._call('POST', 'b/{}/o/{}/compose', bucket, key,
                   destinationPredefinedAcl=acl, json={'sourceObjects': source,
                                                       "kind": "storage#composeRequest",
                                                       'destination': {'name': key, 'bucket': bucket}})

    @_tracemethod
    def copy(self, path1, path2, acl=None):
        """Duplicate remote file
        """
        b1, k1 = split_path(path1)
        b2, k2 = split_path(path2)
        out = self._call('POST', 'b/{}/o/{}/rewriteTo/b/{}/o/{}', b1, k1, b2, k2,
                         destinationPredefinedAcl=acl)
        while out.json()['done'] is not True:
            out = self._call('POST', 'b/{}/o/{}/rewriteTo/b/{}/o/{}', b1, k1, b2, k2,
                             rewriteToken=out['rewriteToken'], destinationPredefinedAcl=acl)

    @_tracemethod
    def mv(self, path1, path2, acl=None):
        """Simulate file move by copy and remove"""
        self.copy(path1, path2, acl)
        self.rm(path1)

    @_tracemethod
    def rm(self, path, recursive=False):
        """Delete keys.

        If a list, batch-delete all keys in one go (can span buckets)

        Returns whether operation succeeded (a list if input was a list)

        If recursive, delete all keys given by walk(path)
        """
        if isinstance(path, (tuple, list)):
            template = ('\n--===============7330845974216740156==\n'
                        'Content-Type: application/http\n'
                        'Content-Transfer-Encoding: binary\n'
                        'Content-ID: <b29c5de2-0db4-490b-b421-6a51b598bd11+{i}>'
                        '\n\nDELETE /storage/v1/b/{bucket}/o/{key} HTTP/1.1\n'
                        'Content-Type: application/json\n'
                        'accept: application/json\ncontent-length: 0\n')
            body = "".join([template.format(i=i+1, bucket=p.split('/', 1)[0],
                            key=quote_plus(p.split('/', 1)[1]))
                            for i, p in enumerate(path)])
            r = self._call('POST', 'https://www.googleapis.com/batch',
                           headers={'Content-Type': 'multipart/mixed; boundary="===============7330845974216740156=="'},
                           data=body + "\n--===============7330845974216740156==--")

            boundary = r.headers['Content-Type'].split('=', 1)[1]
            parents = {posixpath.dirname(norm_path(p)) for p in path}
            [self.invalidate_cache(parent) for parent in parents]
            return ['200 OK' in c or '204 No Content' in c for c in
                    r.text.split(boundary)][1:-1]
        elif recursive:
            return self.rm(self.walk(path))
        else:
            bucket, key = split_path(path)
            self._call('DELETE', "b/{}/o/{}", bucket, key)
            self.invalidate_cache(posixpath.dirname(norm_path(path)))
            return True

    @_tracemethod
    def open(self, path, mode='rb', block_size=None, acl=None,
             consistency=None, metadata=None):
        """
        See ``GCSFile``.

        consistency: None or str
            If None, use default for this instance
        """
        if block_size is None:
            block_size = self.default_block_size
        const = consistency or self.consistency
        if 'b' in mode:
            return GCSFile(self, path, mode, block_size, consistency=const,
                           metadata=metadata)
        else:
            mode = mode.replace('t', '') + 'b'
            return io.TextIOWrapper(
                GCSFile(self, path, mode, block_size, consistency=const,
                        metadata=metadata))

    @_tracemethod
    def touch(self, path, acl=None, metadata=None):
        """Create empty file

        acl, metadata: passed on to open() and then GCSFile
        """
        with self.open(path, 'wb', acl=acl, metadata=metadata):
            pass

    @_tracemethod
    def read_block(self, fn, offset, length, delimiter=None):
        """ Read a block of bytes from a GCS file

        Starting at ``offset`` of the file, read ``length`` bytes.  If
        ``delimiter`` is set then we ensure that the read starts and stops at
        delimiter boundaries that follow the locations ``offset`` and ``offset
        + length``.  If ``offset`` is zero then we start at zero.  The
        bytestring returned WILL include the end delimiter string.

        If offset+length is beyond the eof, reads to eof.

        Parameters
        ----------
        fn: string
            Path to filename on GCS
        offset: int
            Byte offset to start read
        length: int
            Number of bytes to read
        delimiter: bytes (optional)
            Ensure reading starts and stops at delimiter bytestring

        Examples
        --------
        >>> gcs.read_block('data/file.csv', 0, 13)  # doctest: +SKIP
        b'Alice, 100\\nBo'
        >>> gcs.read_block('data/file.csv', 0, 13, delimiter=b'\\n')  # doctest: +SKIP
        b'Alice, 100\\nBob, 200\\n'

        Use ``length=None`` to read to the end of the file.
        >>> gcs.read_block('data/file.csv', 0, None, delimiter=b'\\n')  # doctest: +SKIP
        b'Alice, 100\\nBob, 200\\nCharlie, 300'

        See Also
        --------
        distributed.utils.read_block
        """
        with self.open(fn, 'rb') as f:
            size = f.size
            if length is None:
                length = size
            if offset + length > size:
                length = size - offset
            bytes = read_block(f, offset, length, delimiter)
        return bytes

    def __getstate__(self):
        d = self.__dict__.copy()
        d["_listing_cache"] = {}
        logger.debug("Serialize with state: %s", d)
        return d

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.connect(self.token)


GCSFileSystem.load_tokens()


class GCSFile:

    @_tracemethod
    def __init__(self, gcsfs, path, mode='rb', block_size=DEFAULT_BLOCK_SIZE,
                 acl=None, consistency='md5', metadata=None):
        """
        Open a file.

        Parameters
        ----------
        gcsfs: instance of GCSFileSystem
        path: str
            location in GCS, like 'bucket/path/to/file'
        mode: str
            Normal file modes. Currently only 'wb' amd 'rb'.
        block_size: int
            Buffer size for reading or writing
        acl: str
            ACL to apply, if any, one of ``ACLs``. New files are normally
            "bucketownerfullcontrol", but a default can be configured per
            bucket.
        consistency: str, 'none', 'size', 'md5'
            Check for success in writing, applied at file close.
            'size' ensures that the number of bytes reported by GCS matches
            the number we wrote; 'md5' does a full checksum. Any value other
            than 'size' or 'md5' is assumed to mean no checking.
        metadata: dict
            Custom metadata, in key/value pairs, added at file creation
        """
        bucket, key = split_path(path)
        if not key:
            raise OSError('Attempt to open a bucket')
        self.gcsfs = gcsfs
        self.bucket = bucket
        self.key = key
        self.metadata = metadata
        self.mode = mode
        self.blocksize = block_size
        self.cache = b""
        self.loc = 0
        self.acl = acl
        self.end = None
        self.start = None
        self.closed = False
        self.trim = True
        self.consistency = consistency
        if self.consistency == 'md5':
            self.md5 = md5()
        if mode not in {'rb', 'wb'}:
            raise NotImplementedError('File mode not supported')
        if mode == 'rb':
            self.details = gcsfs.info(path)
            self.size = self.details['size']
        else:
            if block_size < GCS_MIN_BLOCK_SIZE:
                warnings.warn('Setting block size to minimum value, 2**18')
                self.blocksize = GCS_MIN_BLOCK_SIZE
            self.buffer = io.BytesIO()
            self.offset = 0
            self.forced = False
            self.location = None

    def info(self):
        """ File information about this path """
        return self.details

    def url(self):
        """ HTTP link to this file's data """
        return self.details['mediaLink']

    def tell(self):
        """ Current file location """
        return self.loc

    @_tracemethod
    def seek(self, loc, whence=0):
        """ Set current file location

        Parameters
        ----------
        loc : int
            byte location
        whence : {0, 1, 2}
            from start of file, current location or end of file, resp.
        """
        if not self.mode == 'rb':
            raise ValueError('Seek only available in read mode')
        if whence == 0:
            nloc = loc
        elif whence == 1:
            nloc = self.loc + loc
        elif whence == 2:
            nloc = self.size + loc
        else:
            raise ValueError(
                "invalid whence (%s, should be 0, 1 or 2)" % whence)
        if nloc < 0:
            raise ValueError('Seek before start of file')
        self.loc = nloc
        return self.loc

    def readline(self, length=-1):
        """
        Read and return a line from the stream.

        If length is specified, at most size bytes will be read.
        """
        self._fetch(self.loc, self.loc + 1)
        while True:
            found = self.cache[self.loc - self.start:].find(b'\n') + 1
            if 0 < length < found:
                return self.read(length)
            if found:
                return self.read(found)
            if self.end > self.size:
                return self.read(length)
            self._fetch(self.start, self.end + self.blocksize)

    def __next__(self):
        """ Simulate iterating over lines """
        data = self.readline()
        if data:
            return data
        else:
            raise StopIteration

    next = __next__

    def __iter__(self):
        return self

    def readlines(self):
        """ Return all lines in a file as a list """
        return list(self)

    def write(self, data):
        """
        Write data to buffer.

        Buffer only sent to GCS on flush() or if buffer is greater than
        or equal to blocksize.

        Parameters
        ----------
        data : bytes
            Set of bytes to be written.
        """
        if self.mode not in {'wb', 'ab'}:
            raise ValueError('File not in write mode')
        if self.closed:
            raise ValueError('I/O operation on closed file.')
        if self.forced:
            raise ValueError('This file has been force-flushed, can only close')
        out = self.buffer.write(ensure_writable(data))
        self.loc += out
        if self.buffer.tell() >= self.blocksize:
            self.flush()
        return out

    @_tracemethod
    def flush(self, force=False):
        """
        Write buffered data to GCS.

        Uploads the current buffer, if it is larger than the block-size, or if
        the file is being closed.

        Parameters
        ----------
        force : bool
            When closing, write the last block even if it is smaller than
            blocks are allowed to be. Disallows further writing to this file.
        """

        if self.closed:
            raise ValueError('Flush on closed file')
        if force and self.forced:
            raise ValueError("Force flush cannot be called more than once")

        if self.mode not in {'wb', 'ab'}:
            assert not hasattr(self, "buffer"), "flush on read-mode file with non-empty buffer"
            return
        if self.buffer.tell() == 0 and not force:
            # no data in the buffer to write
            return
        if self.buffer.tell() < GCS_MIN_BLOCK_SIZE and not force:
            logger.debug(
                "flush(force=False) with buffer (%i) < min size (2 ** 18), "
                "skipping block upload.", self.buffer.tell()
            )
            return

        if not self.offset:
            if force and self.buffer.tell() <= self.blocksize:
                # Force-write a buffer below blocksize with a single write
                self._simple_upload()
            elif not force and self.buffer.tell() <= self.blocksize:
                # Defer initialization of multipart upload, *may* still
                # be able to simple upload.
                return
            else:
                # At initialize a multipart upload, setting self.location
                self._initiate_upload()

        if self.location is not None:
            # Continue with multipart upload has been initialized
            self._upload_chunk(final=force)

        if force:
            self.forced = True

    @_tracemethod
    def _upload_chunk(self, final=False):
        """ Write one part of a multi-block file upload """
        self.buffer.seek(0)
        data = self.buffer.read()
        head = {}
        l = self.buffer.tell()
        if final:
            if l:
                head['Content-Range'] = 'bytes %i-%i/%i' % (
                    self.offset, self.offset + l - 1, self.offset + l)
            else:
                # closing when buffer is empty
                head['Content-Range'] = 'bytes */%i' % self.offset
                data = None
        else:
            assert l >= GCS_MIN_BLOCK_SIZE, "Non-final chunk write below min size."
            head['Content-Range'] = 'bytes %i-%i/*' % (
                self.offset, self.offset + l - 1)
        head.update({'Content-Type': 'application/octet-stream',
                     'Content-Length': str(l)})
        r = self.gcsfs._call('POST', self.location,
                             uploadType='resumable', headers=head, data=data)
        if 'Range' in r.headers:
            assert not final, "Response looks like upload is partial"
            shortfall = (self.offset + l - 1) - int(
                    r.headers['Range'].split('-')[1])
            if shortfall:
                if self.consistency == 'md5':
                    self.md5.update(data[:-shortfall])
                self.buffer = io.BytesIO(data[-shortfall:])
                self.buffer.seek(shortfall)
            else:
                if self.consistency == 'md5':
                    self.md5.update(data)
                self.buffer = io.BytesIO()
            self.offset += l - shortfall
        else:
            assert final, "Response looks like upload is over"
            size, md5 = int(r.json()['size']), r.json()['md5Hash']
            if self.consistency == 'size':
                assert size == self.buffer.tell() + self.offset, "Size mismatch"
            if self.consistency == 'md5':
                assert b64encode(
                    self.md5.digest()) == md5.encode(), "MD5 checksum failed"
            self.buffer = io.BytesIO()
            self.offset += l

    @_tracemethod
    def _initiate_upload(self):
        """ Create multi-upload """
        r = self.gcsfs._call('POST', 'https://www.googleapis.com/upload/storage'
                                     '/v1/b/%s/o' % quote_plus(self.bucket),
                             uploadType='resumable',
                             json={'name': self.key, 'metadata': self.metadata})
        self.location = r.headers['Location']

    @_tracemethod
    def _simple_upload(self):
        """One-shot upload, less than 5MB"""
        self.buffer.seek(0)
        data = self.buffer.read()
        path = ('https://www.googleapis.com/upload/storage/v1/b/%s/o'
                % quote_plus(self.bucket))
        metadata = {'name': self.key}
        if self.metadata is not None:
            metadata['metadata'] = self.metadata
        metadata = json.dumps(metadata)
        data = ('--==0=='
                '\nContent-Type: application/json; charset=UTF-8'
                '\n\n' + metadata +
                '\n--==0=='
                '\nContent-Type: application/octet-stream'
                '\n\n').encode() + data + b'\n--==0==--'
        r = self.gcsfs._call('POST', path,
                             uploadType='multipart',
                             headers={'Content-Type': 'multipart/related; boundary="==0=="'},
                             data=data)
        size, md5 = int(r.json()['size']), r.json()['md5Hash']
        if self.consistency == 'size':
            assert size == self.buffer.tell(), "Size mismatch"
        if self.consistency == 'md5':
            self.md5.update(data)
            assert b64encode(self.md5.digest()) == md5.encode(), "MD5 checksum failed"

    @_tracemethod
    def _fetch(self, start, end):
        """ Get bytes between start and end, if not already in cache

        Will read ahead by blocksize bytes.
        """
        if self.start is None and self.end is None:
            # First read
            self.start = start
            self.end = end + self.blocksize
            self.cache = self._fetch_range(self.details, self.start, self.end)
        if start < self.start:
            if self.end - end > self.blocksize:
                self.start = start
                self.end = end + self.blocksize
                self.cache = self._fetch_range(self.details, self.start, self.end)
            else:
                new = self._fetch_range(self.details, start, self.start)
                self.start = start
                self.cache = new + self.cache
        if end > self.end:
            if self.end > self.size:
                return
            if end - self.end > self.blocksize:
                self.start = start
                self.end = end + self.blocksize
                self.cache = self._fetch_range(self.details, self.start, self.end)
            else:
                new = self._fetch_range(self.details, self.end, end + self.blocksize)
                self.end = end + self.blocksize
                self.cache = self.cache + new

    def read(self, length=-1):
        """
        Return data from cache, or fetch pieces as necessary

        Parameters
        ----------
        length : int (-1)
            Number of bytes to read; if <0, all remaining bytes.
        """
        if self.mode != 'rb':
            raise ValueError('File not in read mode')
        if length < 0:
            length = self.size
        if self.closed:
            raise ValueError('I/O operation on closed file.')
        self._fetch(self.loc, self.loc + length)
        out = self.cache[self.loc - self.start:
                         self.loc - self.start + length]
        self.loc += len(out)
        if self.trim:
            num = (self.loc - self.start) // self.blocksize - 1
            if num > 0:
                self.start += self.blocksize * num
                self.cache = self.cache[self.blocksize * num:]
        return out

    @_tracemethod
    def close(self):
        """ Close file

        Finalizes writes, discards cache
        """
        if self.closed:
            return
        if self.mode == 'rb':
            self.cache = None
        else:
            if not self.forced:
                self.flush(force=True)
            else:
                logger.debug("close with forced=True, bypassing final flush.")
                assert self.buffer.tell() == 0

            self.gcsfs.invalidate_cache(
                posixpath.dirname("/".join([self.bucket, self.key])))
        self.closed = True

    def readable(self):
        """Return whether the GCSFile was opened for reading"""
        return self.mode == 'rb'

    def seekable(self):
        """Return whether the GCSFile is seekable (only in read mode)"""
        return self.readable()

    def writable(self):
        """Return whether the GCSFile was opened for writing"""
        return self.mode in {'wb', 'ab'}

    @_tracemethod
    def __del__(self):
        self.close()

    def __str__(self):
        return "<GCSFile %s/%s>" % (self.bucket, self.key)

    __repr__ = __str__

    @_tracemethod
    def __enter__(self):
        return self

    @_tracemethod
    def __exit__(self, *args):
        self.close()


    @_tracemethod
    def _fetch_range(self, obj_dict, start=None, end=None):
        """ Get data from GCS

        obj_dict : an entry from ls() or info()
        start, end : None or integers
            if not both None, fetch only given range
        """
        if start is not None or end is not None:
            start = start or 0
            end = end or 0
            head = {'Range': 'bytes=%i-%i' % (start, end - 1)}
        else:
            head = None
        try:
            r = self.gcsfs._call('GET', obj_dict['mediaLink'],
                             headers=head)
            data = r.content
            return data
        except RuntimeError as e:
            if 'not satisfiable' in str(e):
                return b''


def put_object(credentials, bucket, name, data, session):
    """ Simple put, up to 5MB of data

    credentials : from auth()
    bucket : string
    name : object name
    data : binary
    session: requests.Session instance
    """
    out = session.post('https://www.googleapis.com/upload/storage/'
                       'v1/b/%s/o?uploadType=media&name=%s' % (
                           quote_plus(bucket), quote_plus(name)),
                       headers={'Authorization': 'Bearer ' +
                                                 credentials.access_token,
                                'Content-Type': 'application/octet-stream',
                                'Content-Length': len(data)}, data=data)
    assert out.status_code == 200


def ensure_writable(b):
    if PY2 and isinstance(b, array.array):
        return b.tostring()
    return b
