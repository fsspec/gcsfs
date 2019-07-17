# -*- coding: utf-8 -*-
"""
Google Cloud Storage pythonic interface
"""
import fsspec

import decorator

import array
from base64 import b64encode, b64decode
import google.auth as gauth
import google.auth.compute_engine
import google.auth.credentials
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
            raise FileNotFoundError
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


class GCSFileSystem(fsspec.AbstractFileSystem):
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

    GCS *does not* include  "directory" objects but instead generates
    directories by splitting
    [object names](https://cloud.google.com/storage/docs/key-terms).
    This means that, for example,
    a directory does not need to exist for an object to be created within it.
    Creating an object implicitly creates it's parent directories, and removing
    all objects from a directory implicitly deletes the empty directory.

    `GCSFileSystem` generates listing entries for these implied directories in
    listing apis with the  object properies:

        - "name" : string
            The "{bucket}/{name}" path of the dir, used in calls to
            GCSFileSystem or GCSFile.
        - "bucket" : string
            The name of the bucket containing this object.
        - "kind" : 'storage#object'
        - "size" : 0
        - "storageClass" : 'DIRECTORY'
        - type: 'directory' (fsspec compat)

    GCSFileSystem maintains a per-implied-directory cache of object listings and
    fulfills all object information and listing requests from cache. This implied, for example, that objects
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
    protocol = 'gcs', 'gs'

    def __init__(self, project=DEFAULT_PROJECT, access='full_control',
                 token=None, block_size=None, consistency='none',
                 cache_timeout=None, secure_serialize=True,
                 check_connection=True, requests_timeout=None, **kwargs):
        super().__init__(self, **kwargs)
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
        self.requests_timeout = requests_timeout
        self.check_credentials = check_connection
        self._listing_cache = {}
        self.session = None
        self.connect(method=token)

        if not secure_serialize:
            self.token = self.session.credentials

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
        elif isinstance(token, google.auth.credentials.Credentials):
            credentials = token
        else:
            raise ValueError('Token format not understood')
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
        r = None

        if not path.startswith('http'):
            path = self.base + path

        if args:
            path = path.format(*[quote_plus(p) for p in args])

        for retry in range(self.retries):
            try:
                if retry > 0:
                    time.sleep(min(random.random() + 2**(retry-1), 32))
                r = self.session.request(method, path,
                                         params=kwargs, json=json, headers=headers, data=data, timeout=self.requests_timeout)
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
        return [b["name"] for b in self._list_buckets()]

    @staticmethod
    def _process_object(bucket, object_metadata):
        """Process object resource into gcsfs object information format.
        
        Process GCS object resource via type casting and attribute updates to
        the cache-able gcsfs object information format. Returns an updated copy
        of the object resource.

        (See https://cloud.google.com/storage/docs/json_api/v1/objects#resource)
        """
        result = dict(object_metadata)
        result["size"] = int(object_metadata.get("size", 0))
        result["name"] = posixpath.join(bucket, object_metadata["name"])
        result['type'] = 'file'

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
                          delimiter="/", prefix=prefix, maxResults=max_results
                          ).json()

        assert page["kind"] == "storage#objects"
        prefixes.extend(page.get("prefixes", []))
        items.extend([i for i in page.get("items", [])
                      if prefix is None
                      or i['name'].rstrip('/') == prefix.rstrip('/')
                      or i['name'].startswith(prefix.rstrip('/') + '/')])
        next_page_token = page.get('nextPageToken', None)

        while next_page_token is not None:
            page = self._call('GET', 'b/{}/o/', bucket,
                              delimiter="/", prefix=prefix,
                              maxResults=max_results, pageToken=next_page_token
                              ).json()

            assert page["kind"] == "storage#objects"
            prefixes.extend(page.get("prefixes", []))
            items.extend([
                i for i in page.get("items", [])
            ])
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

        return [{'name': i['name'] + '/', 'size': 0, 'type': "directory"}
                for i in items]

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
            bucket name. If contains '/' (i.e., looks like subdir), will
            have no effect because GCS doesn't have real directories.
        acl: string, one of bACLs
            access for the bucket itself
        default_acl: str, one of ACLs
            default ACL for objects created in this bucket
        """
        if bucket in ['', '/']:
            raise ValueError('Cannot create root bucket')
        if '/' in bucket:
            return
        self._call('post', 'b/', predefinedAcl=acl, project=self.project,
                   predefinedDefaultObjectAcl=default_acl,
                   json={"name": bucket})
        self.invalidate_cache(bucket)

    @_tracemethod
    def rmdir(self, bucket):
        """Delete an empty bucket

        Parameters
        ----------
        bucket: str
            bucket name. If contains '/' (i.e., looks like subdir), will
            have no effect because GCS doesn't have real directories.
        """
        if '/' in bucket:
            return
        self._call('delete', 'b/' + bucket)
        self.invalidate_cache(bucket)

    @_tracemethod
    def ls(self, path, detail=False):
        """List objects under the given '/{bucket}/{prefix} path."""
        path = norm_path(path)

        if path in ['/', '']:
            if detail:
                return self._list_buckets()
            else:
                return self.buckets
        elif path.endswith("/"):
            return self._ls(path, detail)
        else:
            combined_listing = self._ls(path, detail) + self._ls(path + "/",
                                                                 detail)
            if detail:
                combined_entries = dict(
                    (l["name"], l) for l in combined_listing)
                combined_entries.pop(path + "/", None)
                return list(combined_entries.values())
            else:
                return list(set(combined_listing) - {path + "/"})

    @_tracemethod
    def _ls(self, path, detail=False):
        listing = self._list_objects(path)
        bucket, key = split_path(path)

        item_details = listing["items"]

        pseudodirs = [{
                'bucket': bucket,
                'name': bucket + "/" + prefix,
                'kind': 'storage#object',
                'size': 0,
                'storageClass': 'DIRECTORY',
                'type': 'directory'
            }
            for prefix in listing["prefixes"]
        ]
        out = item_details + pseudodirs
        if detail:
            return out
        else:
            return sorted([o['name'] for o in out])

    @staticmethod
    def url(path):
        """ Get HTTP URL of the given path """
        u = 'https://www.googleapis.com/download/storage/v1/b/{}/o/{}?alt=media'
        bucket, object = split_path(path)
        object = quote_plus(object)
        return u.format(bucket, object)

    @_tracemethod
    def cat(self, path):
        """ Simple one-shot get of file data """
        u2 = self.url(path)
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

    def getxattr(self, path, attr):
        """Get user-defined metadata attribute"""
        meta = self.info(path).get('metadata', {})
        return meta[attr]

    def setxattrs(self, path, content_type=None, content_encoding=None,
                  **kwargs):
        """ Set/delete/add writable metadata attributes

        Parameters
        ---------
        content_type: str
            If not None, set the content-type to this value
        content_encoding: str
            If not None, set the content-encoding.
            See https://cloud.google.com/storage/docs/transcoding
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
                            fields='metadata', json=i_json
                            ).json()
        self.info(path)['metadata'] = o_json.get('metadata', {})
        return o_json.get('metadata', {})

    @_tracemethod
    def merge(self, path, paths, acl=None):
        """Concatenate objects within a single bucket"""
        bucket, key = split_path(path)
        source = [{'name': split_path(p)[1]} for p in paths]
        self._call('POST', 'b/{}/o/{}/compose', bucket, key,
                   destinationPredefinedAcl=acl,
                   json={'sourceObjects': source,
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
            out = self._call(
                'POST', 'b/{}/o/{}/rewriteTo/b/{}/o/{}', b1, k1, b2, k2,
                rewriteToken=out['rewriteToken'], destinationPredefinedAcl=acl)

    @_tracemethod
    def rm(self, path, recursive=False):
        """Delete keys.

        If a list, batch-delete all keys in one go (can span buckets)

        Returns whether operation succeeded (a list if input was a list)

        If recursive, delete all keys given by find(path)
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
            r = self._call(
                'POST', 'https://www.googleapis.com/batch',
                headers={'Content-Type': 'multipart/mixed; boundary="=========='
                                         '=====7330845974216740156=="'},
                data=body + "\n--===============7330845974216740156==--")

            boundary = r.headers['Content-Type'].split('=', 1)[1]
            parents = {posixpath.dirname(norm_path(p)) for p in path}
            [self.invalidate_cache(parent) for parent in parents]
            return ['200 OK' in c or '204 No Content' in c for c in
                    r.text.split(boundary)][1:-1]
        elif recursive:
            return self.rm(self.find(path))
        else:
            bucket, key = split_path(path)
            self._call('DELETE', "b/{}/o/{}", bucket, key)
            self.invalidate_cache(posixpath.dirname(norm_path(path)))
            return True

    @_tracemethod
    def _open(self, path, mode='rb', block_size=None, acl=None,
              consistency=None, metadata=None, autocommit=True, **kwargs):
        """
        See ``GCSFile``.

        consistency: None or str
            If None, use default for this instance
        """
        if block_size is None:
            block_size = self.default_block_size
        const = consistency or self.consistency
        return GCSFile(self, path, mode, block_size, consistency=const,
                       metadata=metadata, acl=acl, autocommit=autocommit,
                       **kwargs)

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.dircache = {}
        self.connect(self.token)


GCSFileSystem.load_tokens()


class GCSFile(fsspec.spec.AbstractBufferedFile):

    def __init__(self, gcsfs, path, mode='rb', block_size=DEFAULT_BLOCK_SIZE,
                 acl=None, consistency='md5', metadata=None,
                 autocommit=True, **kwargs):
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
        super().__init__(gcsfs, path, mode, block_size, autocommit=autocommit,
                         **kwargs)
        bucket, key = split_path(path)
        if not key:
            raise OSError('Attempt to open a bucket')
        self.gcsfs = gcsfs
        self.bucket = bucket
        self.key = key
        self.metadata = metadata
        self.acl = acl
        self.trim = True
        self.consistency = consistency
        if self.consistency == 'md5':
            self.md5 = md5()
        if mode == 'wb':
            if self.blocksize < GCS_MIN_BLOCK_SIZE:
                warnings.warn('Setting block size to minimum value, 2**18')
                self.blocksize = GCS_MIN_BLOCK_SIZE
            self.location = None

    def info(self):
        """ File information about this path """
        return self.details

    def url(self):
        """ HTTP link to this file's data """
        return self.details['mediaLink']

    @_tracemethod
    def _upload_chunk(self, final=False):
        """ Write one part of a multi-block file upload

        Parameters
        ----------
        final: bool
            Complete and commit upload
        """
        self.buffer.seek(0)
        data = self.buffer.getvalue()
        head = {}
        l = len(data)
        if final and self.autocommit:
            if l:
                head['Content-Range'] = 'bytes %i-%i/%i' % (
                    self.offset, self.offset + l - 1, self.offset + l)
            else:
                # closing when buffer is empty
                head['Content-Range'] = 'bytes */%i' % self.offset
                data = None
        else:
            if l < GCS_MIN_BLOCK_SIZE:
                if not self.autocommit:
                    return
                elif not final:
                    raise ValueError("Non-final chunk write below min size.")
            head['Content-Range'] = 'bytes %i-%i/*' % (
                self.offset, self.offset + l - 1)
        head.update({'Content-Type': 'application/octet-stream',
                     'Content-Length': str(l)})
        r = self.gcsfs._call('POST', self.location,
                             uploadType='resumable', headers=head, data=data)
        if 'Range' in r.headers:
            end = int(r.headers['Range'].split('-')[1])
            shortfall = (self.offset + l - 1) - end
            if shortfall:
                if self.consistency == 'md5':
                    self.md5.update(data[:-shortfall])
                self.buffer = io.BytesIO(data[-shortfall:])
                self.buffer.seek(shortfall)
                self.offset += l - shortfall
                return False
            else:
                if self.consistency == 'md5':
                    self.md5.update(data)
        elif l:
            #
            assert final, "Response looks like upload is over"
            size, md5 = int(r.json()['size']), r.json()['md5Hash']
            if self.consistency == 'size':
                assert size == self.buffer.tell() + self.offset, "Size mismatch"
            if self.consistency == 'md5':
                assert b64encode(
                    self.md5.digest()) == md5.encode(), "MD5 checksum failed"
        else:
            assert final, "Response looks like upload is over"
        return True

    def commit(self):
        """If not auto-committing, finalize file"""
        self.autocommit = True
        self._upload_chunk(final=True)

    @_tracemethod
    def _initiate_upload(self):
        """ Create multi-upload """
        r = self.gcsfs._call('POST', 'https://www.googleapis.com/upload/storage'
                                     '/v1/b/%s/o' % quote_plus(self.bucket),
                             uploadType='resumable',
                             json={'name': self.key, 'metadata': self.metadata})
        self.location = r.headers['Location']

    @_tracemethod
    def discard(self):
        """Cancel in-progress multi-upload

        Should only happen during discarding this write-mode file
        """
        if self.location is None:
            raise ValueError('Cannot cancel upload which has not started')
        uid = re.findall('upload_id=([^&=?]+)', self.location)
        r = self.gcsfs._call('DELETE',
            'https://www.googleapis.com/upload/storage/v1/b/%s/o'
            % quote_plus(self.bucket),
            params={'uploadType': 'resumable', 'upload_id': uid})
        r.raise_for_status()

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
        r = self.gcsfs._call(
            'POST', path,
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
    def _fetch_range(self, start=None, end=None):
        """ Get data from GCS

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
            r = self.gcsfs._call('GET', self.details['mediaLink'],
                                 headers=head)
            data = r.content
            return data
        except RuntimeError as e:
            if 'not satisfiable' in str(e):
                return b''
