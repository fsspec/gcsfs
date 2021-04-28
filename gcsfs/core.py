# -*- coding: utf-8 -*-
"""
Google Cloud Storage pythonic interface
"""
import textwrap
import asyncio
import fsspec

import google.auth as gauth
import google.auth.compute_engine
import google.auth.credentials
from google.auth.exceptions import GoogleAuthError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2 import service_account
from google.auth.transport.requests import Request
import io
import json
import logging
import os
import posixpath
import requests
import pickle
import re
import requests
import threading
import warnings
import random
import weakref

from requests.exceptions import RequestException, ProxyError
from fsspec.asyn import sync_wrapper, sync, AsyncFileSystem
from fsspec.utils import stringify_path, setup_logging
from fsspec.implementations.http import get_client
from .utils import ChecksumError, HttpError, is_retriable
from .checkers import get_consistency_checker, MD5Checker
from . import __version__ as version

logger = logging.getLogger("gcsfs")


if "GCSFS_DEBUG" in os.environ:
    setup_logging(logger=logger, level=os.environ["GCSFS_DEBUG"])


# client created 2018-01-16
not_secret = {
    "client_id": "586241054156-9kst7ltfj66svc342pcn43vp6ta3idin"
    ".apps.googleusercontent.com",
    "client_secret": "xto0LIFYX35mmHF9T1R2QBqT",
}
client_config = {
    "installed": {
        "client_id": not_secret["client_id"],
        "client_secret": not_secret["client_secret"],
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://accounts.google.com/o/oauth2/token",
    }
}
tfile = os.path.join(os.path.expanduser("~"), ".gcs_tokens")
ACLs = {
    "authenticatedread",
    "bucketownerfullcontrol",
    "bucketownerread",
    "private",
    "projectprivate",
    "publicread",
}
bACLs = {
    "authenticatedRead",
    "private",
    "projectPrivate",
    "publicRead",
    "publicReadWrite",
}
DEFAULT_PROJECT = os.environ.get("GCSFS_DEFAULT_PROJECT", "")

GCS_MIN_BLOCK_SIZE = 2 ** 18
GCS_MAX_BLOCK_SIZE = 2 ** 28
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
    s = s.replace("/", "%2F")
    s = s.replace(" ", "%20")
    return s


def norm_path(path):
    """
    Canonicalize path to '{bucket}/{name}' form.

    Used by petastorm, do not remove.
    """
    return "/".join(GCSFileSystem.split_path(path))


async def _req_to_text(r):
    async with r:
        return (await r.read()).decode()


class GoogleCredentials:
    def __init__(self, project, access, token):
        self.scope = "https://www.googleapis.com/auth/devstorage." + access
        self.project = project
        self.access = access
        self.heads = {}

        self.credentials = None
        self.method = None
        self.lock = threading.Lock()
        self.token = token
        self.connect(method=token)

    @classmethod
    def load_tokens(cls):
        """Get "browser" tokens from disc"""
        try:
            with open(tfile, "rb") as f:
                tokens = pickle.load(f)
        except Exception:
            tokens = {}
        GCSFileSystem.tokens = tokens

    @staticmethod
    def _save_tokens():
        try:
            with open(tfile, "wb") as f:
                pickle.dump(GCSFileSystem.tokens, f, 2)
        except Exception as e:
            warnings.warn("Saving token cache failed: " + str(e))

    def _connect_google_default(self):
        credentials, project = gauth.default(scopes=[self.scope])
        msg = textwrap.dedent(
            """\
        User-provided project '{}' does not match the google default project '{}'. Either

          1. Accept the google-default project by not passing a `project` to GCSFileSystem
          2. Configure the default project to match the user-provided project (gcloud config set project)
          3. Use an authorization method other than 'google_default' by providing 'token=...'
        """
        )
        if self.project and self.project != project:
            raise ValueError(msg.format(self.project, project))
        self.project = project
        self.credentials = credentials

    def _connect_cloud(self):
        self.credentials = gauth.compute_engine.Credentials()

    def _connect_cache(self):
        project, access = self.project, self.access
        if (project, access) in self.tokens:
            credentials = self.tokens[(project, access)]
            self.credentials = credentials

    def _dict_to_credentials(self, token):
        """
        Convert old dict-style token.

        Does not preserve access token itself, assumes refresh required.
        """
        try:
            token = service_account.Credentials.from_service_account_info(
                token, scopes=[self.scope]
            )
        except:  # noqa: E722
            # TODO: catch specific exceptions
            # According https://github.com/googleapis/python-cloud-core/blob/master/google/cloud/client.py
            # Scopes required for authenticating with a service. User authentification fails
            # with invalid_scope if scope is specified.
            token = Credentials(
                None,
                refresh_token=token["refresh_token"],
                client_secret=token["client_secret"],
                client_id=token["client_id"],
                token_uri="https://oauth2.googleapis.com/token",
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
            except:  # noqa: E722
                # TODO: catch specific exceptions
                # some other kind of token file
                # will raise exception if is not json
                token = json.load(open(token))
        if isinstance(token, dict):
            credentials = self._dict_to_credentials(token)
        elif isinstance(token, google.auth.credentials.Credentials):
            credentials = token
        else:
            raise ValueError("Token format not understood")
        self.credentials = credentials
        if self.credentials.valid:
            self.credentials.apply(self.heads)

    def maybe_refresh(self):
        # this uses requests and is blocking
        if self.credentials is None:
            return  # anon
        if self.credentials.valid:
            return  # still good
        req = Request(requests.Session())
        with self.lock:
            if self.credentials.valid:
                return  # repeat to avoid race (but don't want lock in common case)
            logger.debug("GCS refresh")
            self.credentials.refresh(req)
            self.apply(self.heads)

    def apply(self, out):
        """Insert credential headers in-place to a dictionary"""
        self.maybe_refresh()
        self.credentials.apply(out)

    def _connect_service(self, fn):
        # raises exception if file does not match expectation
        credentials = service_account.Credentials.from_service_account_file(
            fn, scopes=[self.scope]
        )
        self.credentials = credentials

    def _connect_anon(self):
        self.credentials = None

    def _connect_browser(self):
        flow = InstalledAppFlow.from_client_config(client_config, [self.scope])
        credentials = flow.run_console()
        self.tokens[(self.project, self.access)] = credentials
        self._save_tokens()
        self.credentials = credentials

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
        if method not in [
            "google_default",
            "cache",
            "cloud",
            "token",
            "anon",
            "browser",
            None,
        ]:
            self._connect_token(method)
        elif method is None:
            for meth in ["google_default", "cache", "cloud", "anon"]:
                try:
                    self.connect(method=meth)
                    if self.check_credentials and meth != "anon":
                        self.ls("anaconda-public-data")
                    logger.debug("Connected with method %s", meth)
                    break
                except Exception as e:  # noqa: E722
                    # TODO: catch specific exceptions
                    logger.debug(
                        'Connection with method "%s" failed' % meth, exc_info=e
                    )
        else:
            self.__getattribute__("_connect_" + method)()
            self.method = method


class GCSFileSystem(AsyncFileSystem):
    r"""
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
        project_id to work under. Note that this is not the same as, but often
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
    secure_serialize: bool (deprecated)
    check_connection: bool
        When token=None, gcsfs will attempt various methods of establishing
        credentials, falling back to anon. It is possible for a method to
        find credentials in the system that turn out not to be valid. Setting
        this parameter to True will ensure that an actual operation is
        attempted before deciding that credentials are valid.
    requester_pays : bool, or str default False
        Whether to use requester-pays requests. This will include your
        project ID `project` in requests as the `userPorject`, and you'll be
        billed for accessing data from requester-pays buckets. Optionally,
        pass a project-id here as a string to use that as the `userProject`.
    """

    scopes = {"read_only", "read_write", "full_control"}
    retries = 6  # number of retries on http failure
    base = "https://www.googleapis.com/storage/v1/"
    default_block_size = DEFAULT_BLOCK_SIZE
    protocol = "gcs", "gs"
    async_impl = True

    def __init__(
        self,
        project=DEFAULT_PROJECT,
        access="full_control",
        token=None,
        block_size=None,
        consistency="none",
        cache_timeout=None,
        secure_serialize=True,
        check_connection=False,
        requests_timeout=None,
        requester_pays=False,
        asynchronous=False,
        loop=None,
        timeout=None,
        **kwargs,
    ):
        super().__init__(
            self,
            listings_expiry_time=cache_timeout,
            asynchronous=asynchronous,
            loop=loop,
            **kwargs,
        )
        if access not in self.scopes:
            raise ValueError("access must be one of {}", self.scopes)
        if project is None:
            warnings.warn("GCS project not set - cannot list or create buckets")
        if block_size is not None:
            self.default_block_size = block_size
        self.project = project
        self.requester_pays = requester_pays
        self.consistency = consistency
        self.cache_timeout = cache_timeout or kwargs.pop("listings_expiry_time", None)
        self.requests_timeout = requests_timeout
        self.check_credentials = check_connection
        self.timeout = timeout
        self._session = None

        self.credentials = GoogleCredentials(project, access, token)

        if not self.asynchronous:
            self._session = sync(self.loop, get_client, timeout=self.timeout)
            weakref.finalize(self, self.close_session, self.loop, self._session)

    @staticmethod
    def close_session(loop, session):
        if loop is not None and session is not None:
            if loop.is_running():
                sync(loop, session.close, timeout=0.1)
            else:
                pass

    async def _set_session(self):
        if self._session is None:
            self._session = await get_client()
        return self._session

    @property
    def session(self):
        if self.asynchronous and self._session is None:
            raise RuntimeError("Please await _connect* before anything else")
        return self._session

    @classmethod
    def _strip_protocol(cls, path):
        if isinstance(path, list):
            return [cls._strip_protocol(p) for p in path]
        path = stringify_path(path)
        protos = (cls.protocol,) if isinstance(cls.protocol, str) else cls.protocol
        for protocol in protos:
            if path.startswith(protocol + "://"):
                path = path[len(protocol) + 3 :]
            elif path.startswith(protocol + "::"):
                path = path[len(protocol) + 2 :]
        # use of root_marker to make minimum required path, e.g., "/"
        return path or cls.root_marker

    def _get_params(self, kwargs):
        params = {k: v for k, v in kwargs.items() if v is not None}
        # needed for requester pays buckets
        if self.requester_pays:
            if isinstance(self.requester_pays, str):
                user_project = self.requester_pays
            else:
                user_project = self.project
            params["userProject"] = user_project
        return params

    def _get_headers(self, headers):
        out = {}
        if headers is not None:
            out.update(headers)
        if "User-Agent" not in out:
            out["User-Agent"] = "python-gcsfs/" + version
        self.credentials.apply(out)
        return out

    def _format_path(self, path, args):
        if not path.startswith("http"):
            path = self.base + path

        if args:
            path = path.format(*[quote_plus(p) for p in args])
        return path

    async def _request(
        self, method, path, *args, headers=None, json=None, data=None, **kwargs
    ):
        await self._set_session()
        async with self.session.request(
            method=method,
            url=self._format_path(path, args),
            params=self._get_params(kwargs),
            json=json,
            headers=self._get_headers(headers),
            data=data,
            timeout=self.requests_timeout,
        ) as r:

            status = r.status
            headers = r.headers
            info = r.request_info  # for debug only
            contents = await r.read()

            return status, headers, info, contents

    async def _call(
        self, method, path, *args, json_out=False, info_out=False, **kwargs
    ):
        logger.debug(f"{method.upper()}: {path}, {args}, {kwargs.get('headers')}")

        for retry in range(self.retries):
            try:
                if retry > 0:
                    await asyncio.sleep(min(random.random() + 2 ** (retry - 1), 32))
                status, headers, info, contents = await self._request(
                    method, path, *args, **kwargs
                )
                self.validate_response(status, contents, path, headers)
                break
            except (HttpError, RequestException, GoogleAuthError, ChecksumError) as e:
                if (
                    isinstance(e, HttpError)
                    and e.code == 400
                    and "requester pays" in e.message
                ):
                    msg = "Bucket is requester pays. Set `requester_pays=True` when creating the GCSFileSystem."
                    raise ValueError(msg) from e
                if retry == self.retries - 1:
                    logger.exception("_call out of retries on exception: %s" % e)
                    raise e
                if is_retriable(e):
                    logger.debug("_call retrying after exception: %s" % e)
                    continue
                logger.exception("_call non-retriable exception: %s" % e)
                raise e
        if json_out:
            return json.loads(contents)
        elif info_out:
            return info
        else:
            return headers, contents

    call = sync_wrapper(_call)

    @property
    def buckets(self):
        """Return list of available project buckets."""
        return [
            b["name"] for b in sync(self.loop, self._list_buckets, timeout=self.timeout)
        ]

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
        result["type"] = "file"

        return result

    async def _get_object(self, path):
        """Return object information at the given path."""
        bucket, key = self.split_path(path)

        # Check if parent dir is in listing cache
        listing = self._ls_from_cache(path)
        if listing:
            f = [f for f in listing if f["type"] == "file"]
            if f:
                return f
            # parent is listed, doesn't contain the path
            raise FileNotFoundError(path)

        if not key:
            # Attempt to "get" the bucket root, return error instead of
            # listing.
            raise FileNotFoundError(path)

        res = None
        # Work around various permission settings. Prefer an object get (storage.objects.get), but
        # fall back to a bucket list + filter to object name (storage.objects.list).
        try:
            res = await self._call("GET", "b/{}/o/{}", bucket, key, json_out=True)
        except OSError as e:
            if not str(e).startswith("Forbidden"):
                raise
            resp = await self._call(
                "GET", "b/{}/o/", bucket, json_out=True, prefix=key, maxResults=1
            )
            for item in resp.get("items", []):
                if item["name"] == key:
                    res = item
                    break
            if res is None:
                raise FileNotFoundError(path)
        return self._process_object(bucket, res)

    async def _list_objects(self, path, prefix=""):
        bucket, key = self.split_path(path)
        path = path.rstrip("/")

        try:
            clisting = self._ls_from_cache(path)
            hassubdirs = clisting and any(
                c["name"].rstrip("/") == path and c["type"] == "directory"
                for c in clisting
            )
            if clisting and not hassubdirs:
                return clisting
        except FileNotFoundError:
            # not finding a bucket in list of "my" buckets is OK
            if key:
                raise

        items, prefixes = await self._do_list_objects(path, prefix=prefix)

        pseudodirs = [
            {
                "bucket": bucket,
                "name": bucket + "/" + prefix.strip("/"),
                "size": 0,
                "storageClass": "DIRECTORY",
                "type": "directory",
            }
            for prefix in prefixes
        ]
        if not (items + pseudodirs):
            if key:
                return [await self._get_object(path)]
            else:
                return []
        out = items + pseudodirs
        # Don't cache prefixed/partial listings
        if not prefix:
            self.dircache[path] = out
        return out

    async def _do_list_objects(self, path, max_results=None, delimiter="/", prefix=""):
        """Object listing for the given {bucket}/{prefix}/ path."""
        bucket, _path = self.split_path(path)
        _path = "" if not _path else _path.rstrip("/") + "/"
        prefix = f"{_path}{prefix}" or None

        prefixes = []
        items = []
        page = await self._call(
            "GET",
            "b/{}/o/",
            bucket,
            delimiter=delimiter,
            prefix=prefix,
            maxResults=max_results,
            json_out=True,
        )

        prefixes.extend(page.get("prefixes", []))
        items.extend(page.get("items", []))
        next_page_token = page.get("nextPageToken", None)

        while next_page_token is not None:
            page = await self._call(
                "GET",
                "b/{}/o/",
                bucket,
                delimiter=delimiter,
                prefix=prefix,
                maxResults=max_results,
                pageToken=next_page_token,
                json_out=True,
            )

            assert page["kind"] == "storage#objects"
            prefixes.extend(page.get("prefixes", []))
            items.extend(page.get("items", []))
            next_page_token = page.get("nextPageToken", None)

        items = [self._process_object(bucket, i) for i in items]
        return items, prefixes

    async def _list_buckets(self):
        """Return list of all buckets under the current project."""
        if "" not in self.dircache:
            items = []
            page = await self._call("GET", "b/", project=self.project, json_out=True)

            assert page["kind"] == "storage#buckets"
            items.extend(page.get("items", []))
            next_page_token = page.get("nextPageToken", None)

            while next_page_token is not None:
                page = await self._call(
                    "GET",
                    "b/",
                    project=self.project,
                    pageToken=next_page_token,
                    json_out=True,
                )

                assert page["kind"] == "storage#buckets"
                items.extend(page.get("items", []))
                next_page_token = page.get("nextPageToken", None)

            self.dircache[""] = [
                {"name": i["name"] + "/", "size": 0, "type": "directory"} for i in items
            ]
        return self.dircache[""]

    def invalidate_cache(self, path=None):
        """
        Invalidate listing cache for given path, it is reloaded on next use.

        Parameters
        ----------
        path: string or None
            If None, clear all listings cached else listings at or under given
            path.
        """
        if path is None:
            logger.debug("invalidate_cache clearing cache")
            self.dircache.clear()
        else:
            path = self._strip_protocol(path).rstrip("/")

            while path:
                self.dircache.pop(path, None)
                path = self._parent(path)

    async def _mkdir(
        self, bucket, acl="projectPrivate", default_acl="bucketOwnerFullControl"
    ):
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
        if bucket in ["", "/"]:
            raise ValueError("Cannot create root bucket")
        if "/" in bucket:
            return
        await self._call(
            method="POST",
            path="b/",
            predefinedAcl=acl,
            project=self.project,
            predefinedDefaultObjectAcl=default_acl,
            json={"name": bucket},
            json_out=True,
        )
        self.invalidate_cache(bucket)

    mkdir = sync_wrapper(_mkdir)

    async def _rmdir(self, bucket):
        """Delete an empty bucket

        Parameters
        ----------
        bucket: str
            bucket name. If contains '/' (i.e., looks like subdir), will
            have no effect because GCS doesn't have real directories.
        """
        bucket = bucket.rstrip("/")
        if "/" in bucket:
            return
        await self._call("DELETE", "b/" + bucket, json_out=False)
        self.invalidate_cache(bucket)
        self.invalidate_cache("")

    rmdir = sync_wrapper(_rmdir)

    async def _info(self, path, **kwargs):
        """File information about this path."""
        path = self._strip_protocol(path).rstrip("/")
        # Check directory cache for parent dir
        parent_path = self._parent(path)
        parent_cache = self._ls_from_cache(parent_path)
        bucket, key = self.split_path(path)
        if parent_cache:
            for o in parent_cache:
                if o["name"].rstrip("/") == path:
                    return o
        if self._ls_from_cache(path):
            # this is a directory
            return {
                "bucket": bucket,
                "name": path.rstrip("/"),
                "size": 0,
                "storageClass": "DIRECTORY",
                "type": "directory",
            }
        # Check exact file path
        try:
            return await self._get_object(path)
        except FileNotFoundError:
            pass
        kwargs["detail"] = True  # Force to true for info
        out = await self._ls(path, **kwargs)
        out0 = [o for o in out if o["name"].rstrip("/") == path]
        if out0:
            # exact hit
            return out0[0]
        elif out:
            # other stuff - must be a directory
            return {
                "bucket": bucket,
                "name": path.rstrip("/"),
                "size": 0,
                "storageClass": "DIRECTORY",
                "type": "directory",
            }
        else:
            raise FileNotFoundError(path)

    async def _glob(self, path, prefix="", **kwargs):
        if not prefix:
            # Identify pattern prefixes. Ripped from fsspec.spec.AbstractFileSystem.glob and matches
            # the glob.has_magic patterns.
            indstar = path.find("*") if path.find("*") >= 0 else len(path)
            indques = path.find("?") if path.find("?") >= 0 else len(path)
            indbrace = path.find("[") if path.find("[") >= 0 else len(path)

            ind = min(indstar, indques, indbrace)
            prefix = path[:ind].split("/")[-1]
        return await super()._glob(path, prefix=prefix, **kwargs)

    async def _ls(self, path, detail=False, prefix="", **kwargs):
        """List objects under the given '/{bucket}/{prefix} path."""
        path = self._strip_protocol(path).rstrip("/")

        if path in ["/", ""]:
            out = await self._list_buckets()
        else:
            out = await self._list_objects(path, prefix=prefix)

        if detail:
            return out
        else:
            return sorted([o["name"] for o in out])

    @classmethod
    def url(cls, path):
        """ Get HTTP URL of the given path """
        u = "https://www.googleapis.com/download/storage/v1/b/{}/o/{}?alt=media"
        bucket, object = cls.split_path(path)
        object = quote_plus(object)
        return u.format(bucket, object)

    async def _cat_file(self, path, start=None, end=None):
        """ Simple one-shot get of file data """
        u2 = self.url(path)
        if start or end:
            head = {"Range": "bytes=%i-%s" % (start or 0, end - 1 if end else "")}
        else:
            head = {}
        headers, out = await self._call("GET", u2, headers=head)
        return out

    async def _getxattr(self, path, attr):
        """Get user-defined metadata attribute"""
        meta = (await self._info(path)).get("metadata", {})
        return meta[attr]

    getxattr = sync_wrapper(_getxattr)

    async def _setxattrs(
        self, path, content_type=None, content_encoding=None, **kwargs
    ):
        """Set/delete/add writable metadata attributes

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
        i_json = {"metadata": kwargs}
        if content_type is not None:
            i_json["contentType"] = content_type
        if content_encoding is not None:
            i_json["contentEncoding"] = content_encoding

        bucket, key = self.split_path(path)
        o_json = await self._call(
            "PATCH",
            "b/{}/o/{}",
            bucket,
            key,
            fields="metadata",
            json=i_json,
            json_out=True,
        )
        (await self._info(path))["metadata"] = o_json.get("metadata", {})
        return o_json.get("metadata", {})

    setxattrs = sync_wrapper(_setxattrs)

    async def _merge(self, path, paths, acl=None):
        """Concatenate objects within a single bucket"""
        bucket, key = self.split_path(path)
        source = [{"name": self.split_path(p)[1]} for p in paths]
        await self._call(
            "POST",
            "b/{}/o/{}/compose",
            bucket,
            key,
            destinationPredefinedAcl=acl,
            headers={"Content-Type": "application/json"},
            json={
                "sourceObjects": source,
                "kind": "storage#composeRequest",
                "destination": {"name": key, "bucket": bucket},
            },
        )

    merge = sync_wrapper(_merge)

    async def _cp_file(self, path1, path2, acl=None, **kwargs):
        """Duplicate remote file"""
        b1, k1 = self.split_path(path1)
        b2, k2 = self.split_path(path2)
        out = await self._call(
            "POST",
            "b/{}/o/{}/rewriteTo/b/{}/o/{}",
            b1,
            k1,
            b2,
            k2,
            headers={"Content-Type": "application/json"},
            destinationPredefinedAcl=acl,
            json_out=True,
        )
        while out["done"] is not True:
            out = await self._call(
                "POST",
                "b/{}/o/{}/rewriteTo/b/{}/o/{}",
                b1,
                k1,
                b2,
                k2,
                headers={"Content-Type": "application/json"},
                rewriteToken=out["rewriteToken"],
                destinationPredefinedAcl=acl,
                json_out=True,
            )

    async def _rm_file(self, path):
        bucket, key = self.split_path(path)
        if key:
            await self._call("DELETE", "b/{}/o/{}", bucket, key)
            self.invalidate_cache(posixpath.dirname(self._strip_protocol(path)))
            return True
        else:
            await self._rmdir(path)

    async def _rm_files(self, paths):
        template = (
            "\n--===============7330845974216740156==\n"
            "Content-Type: application/http\n"
            "Content-Transfer-Encoding: binary\n"
            "Content-ID: <b29c5de2-0db4-490b-b421-6a51b598bd11+{i}>"
            "\n\nDELETE /storage/v1/b/{bucket}/o/{key} HTTP/1.1\n"
            "Content-Type: application/json\n"
            "accept: application/json\ncontent-length: 0\n"
        )
        body = "".join(
            [
                template.format(
                    i=i + 1,
                    bucket=p.split("/", 1)[0],
                    key=quote_plus(p.split("/", 1)[1]),
                )
                for i, p in enumerate(paths)
            ]
        )
        headers, content = await self._call(
            "POST",
            "https://www.googleapis.com/batch/storage/v1",
            headers={
                "Content-Type": 'multipart/mixed; boundary="=========='
                '=====7330845974216740156=="'
            },
            data=body + "\n--===============7330845974216740156==--",
        )

        boundary = headers["Content-Type"].split("=", 1)[1]
        parents = [self._parent(p) for p in paths]
        [self.invalidate_cache(parent) for parent in parents + list(paths)]
        txt = content.decode()
        if any(
            not ("200 OK" in c or "204 No Content" in c)
            for c in txt.split(boundary)[1:-1]
        ):
            pattern = '"message": "([^"]+)"'
            out = set(re.findall(pattern, txt))
            raise OSError(out)

    async def _rm(self, path, recursive=False, maxdepth=None, batchsize=20):
        paths = await self._expand_path(path, recursive=recursive, maxdepth=maxdepth)
        files = [p for p in paths if self.split_path(p)[1]]
        dirs = [p for p in paths if not self.split_path(p)[1]]
        exs = await asyncio.gather(
            *(
                [
                    self._rm_files(files[i : i + batchsize])
                    for i in range(0, len(files), batchsize)
                ]
            ),
            return_exceptions=True,
        )
        exs = [ex for ex in exs if ex is not None and "No such object" not in str(ex)]
        if exs:
            raise exs[0]
        await asyncio.gather(*[self._rmdir(d) for d in dirs])

    rm = sync_wrapper(_rm)

    async def _pipe_file(
        self,
        path,
        data,
        metadata=None,
        consistency=None,
        content_type="application/octet-stream",
        chunksize=50 * 2 ** 20,
    ):
        # enforce blocksize should be a multiple of 2**18
        consistency = consistency or self.consistency
        bucket, key = self.split_path(path)
        size = len(data)
        out = None
        if size < 5 * 2 ** 20:
            return await simple_upload(
                self, bucket, key, data, metadata, consistency, content_type
            )
        else:
            location = await initiate_upload(self, bucket, key, content_type, metadata)
            for offset in range(0, len(data), chunksize):
                bit = data[offset : offset + chunksize]
                out = await upload_chunk(
                    self, location, bit, offset, size, content_type
                )

        checker = get_consistency_checker(consistency)
        checker.update(data)
        checker.validate_json_response(out)
        self.invalidate_cache(self._parent(path))

    async def _put_file(
        self,
        lpath,
        rpath,
        metadata=None,
        consistency=None,
        content_type="application/octet-stream",
        chunksize=50 * 2 ** 20,
        **kwargs,
    ):
        # enforce blocksize should be a multiple of 2**18
        if os.path.isdir(lpath):
            return
        consistency = consistency or self.consistency
        checker = get_consistency_checker(consistency)
        bucket, key = self.split_path(rpath)
        with open(lpath, "rb") as f0:
            size = f0.seek(0, 2)
            f0.seek(0)
            if size < 5 * 2 ** 20:
                return await simple_upload(
                    self,
                    bucket,
                    key,
                    f0.read(),
                    consistency=consistency,
                    metadatain=metadata,
                    content_type=content_type,
                )
            else:
                location = await initiate_upload(
                    self, bucket, key, content_type, metadata
                )
                offset = 0
                while True:
                    bit = f0.read(chunksize)
                    if not bit:
                        break
                    out = await upload_chunk(
                        self, location, bit, offset, size, content_type
                    )
                    offset += len(bit)
                    checker.update(bit)

            checker.validate_json_response(out)
            self.invalidate_cache(self._parent(rpath))

    async def _isdir(self, path):
        try:
            return (await self._info(path))["type"] == "directory"
        except IOError:
            return False

    async def _find(self, path, withdirs=False, detail=False, prefix="", **kwargs):
        path = self._strip_protocol(path)
        bucket, key = self.split_path(path)
        out, _ = await self._do_list_objects(
            path,
            delimiter=None,
            prefix=prefix,
        )
        if not out and key:
            try:
                out = [
                    await self._get_object(
                        path,
                    )
                ]
            except FileNotFoundError:
                out = []
        dirs = []
        sdirs = set()
        cache_entries = {}
        for o in out:
            par = o["name"]
            while par:
                par = self._parent(par)
                if par not in sdirs:
                    if len(par) < len(path):
                        break
                    sdirs.add(par)
                    dirs.append(
                        {
                            "Key": self.split_path(par)[1],
                            "Size": 0,
                            "name": par,
                            "StorageClass": "DIRECTORY",
                            "type": "directory",
                            "size": 0,
                        }
                    )
                # Don't cache "folder-like" objects (ex: "Create Folder" in GCS console) to prevent
                # masking subfiles in subsequent requests.
                if not o["name"].endswith("/"):
                    cache_entries.setdefault(par, []).append(o)
        self.dircache.update(cache_entries)

        if withdirs:
            out = sorted(out + dirs, key=lambda x: x["name"])

        if detail:
            return {o["name"]: o for o in out}
        return [o["name"] for o in out]

    async def _get_file(self, rpath, lpath, **kwargs):
        if await self._isdir(rpath):
            return
        u2 = self.url(rpath)
        headers = kwargs.pop("headers", {})
        consistency = kwargs.pop("consistency", self.consistency)
        if "User-Agent" not in headers:
            headers["User-Agent"] = "python-gcsfs/" + version
        self.credentials.apply(headers)

        # needed for requester pays buckets
        if self.requester_pays:
            if isinstance(self.requester_pays, str):
                user_project = self.requester_pays
            else:
                user_project = self.project
            kwargs["userProject"] = user_project

        async with self.session.get(
            url=u2,
            params=kwargs,
            headers=headers,
            timeout=self.requests_timeout,
        ) as r:
            r.raise_for_status()
            checker = get_consistency_checker(consistency)

            os.makedirs(os.path.dirname(lpath), exist_ok=True)
            with open(lpath, "wb") as f2:
                while True:
                    data = await r.content.read(4096 * 32)
                    if not data:
                        break
                    f2.write(data)
                    checker.update(data)

            checker.validate_http_response(r)

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        cache_options=None,
        acl=None,
        consistency=None,
        metadata=None,
        autocommit=True,
        **kwargs,
    ):
        """
        See ``GCSFile``.

        consistency: None or str
            If None, use default for this instance
        """
        if block_size is None:
            block_size = self.default_block_size
        const = consistency or self.consistency
        return GCSFile(
            self,
            path,
            mode,
            block_size,
            cache_options=cache_options,
            consistency=const,
            metadata=metadata,
            acl=acl,
            autocommit=autocommit,
            **kwargs,
        )

    @classmethod
    def split_path(cls, path):
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
        """
        path = cls._strip_protocol(path).lstrip("/")
        if "/" not in path:
            return path, ""
        else:
            return path.split("/", 1)

    def validate_response(self, status, content, path, headers=None):
        """
        Check the requests object r, raise error if it's not ok.

        Parameters
        ----------
        r: requests response object
        path: associated URL path, for error messages
        """
        if status >= 400:
            error = None
            if hasattr(content, "decode"):
                content = content.decode()
            try:
                error = json.loads(content)["error"]
                msg = error["message"]
            except:  # noqa: E722
                # TODO: limit to appropriate exceptions
                msg = content

            if status == 404:
                raise FileNotFoundError
            elif status == 403:
                raise IOError("Forbidden: %s\n%s" % (path, msg))
            elif status == 502:
                raise ProxyError()
            elif "invalid" in str(msg):
                raise ValueError("Bad Request: %s\n%s" % (path, msg))
            elif error:
                raise HttpError(error)
            elif status:
                raise HttpError(
                    {
                        "code": status,
                        "message": msg,
                    }
                )  # text-like
            else:
                raise RuntimeError(msg)
        else:
            if self.consistency != "md5":
                return None
            elif headers is not None and "X-Goog-Hash" in headers:
                checker = MD5Checker()
                checker.update(content)
                checker.validate_headers(headers)


GoogleCredentials.load_tokens()


class GCSFile(fsspec.spec.AbstractBufferedFile):
    def __init__(
        self,
        gcsfs,
        path,
        mode="rb",
        block_size=DEFAULT_BLOCK_SIZE,
        autocommit=True,
        cache_type="readahead",
        cache_options=None,
        acl=None,
        consistency="md5",
        metadata=None,
        content_type=None,
        timeout=None,
        **kwargs,
    ):
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
        consistency: str, 'none', 'size', 'md5', 'crc32c'
            Check for success in writing, applied at file close.
            'size' ensures that the number of bytes reported by GCS matches
            the number we wrote; 'md5' does a full checksum. Any value other
            than 'size' or 'md5' or 'crc32' is assumed to mean no checking.
        content_type: str
            default is `application/octet-stream`. See the list of available
            content types at https://www.iana.org/assignments/media-types/media-types.txt
        metadata: dict
            Custom metadata, in key/value pairs, added at file creation
        timeout: int
            Timeout seconds for the asynchronous callback.
        """
        super().__init__(
            gcsfs,
            path,
            mode,
            block_size,
            autocommit=autocommit,
            cache_type=cache_type,
            cache_options=cache_options,
            **kwargs,
        )
        bucket, key = self.fs.split_path(path)
        if not key:
            raise OSError("Attempt to open a bucket")
        self.gcsfs = gcsfs
        self.bucket = bucket
        self.key = key
        self.acl = acl
        self.checker = get_consistency_checker(consistency)

        det = getattr(self, "details", {})  # only exists in read mode
        self.content_type = content_type or det.get(
            "contentType", "application/octet-stream"
        )
        self.metadata = metadata or det.get("metadata", {})
        self.timeout = timeout
        if mode == "wb":
            if self.blocksize < GCS_MIN_BLOCK_SIZE:
                warnings.warn("Setting block size to minimum value, 2**18")
                self.blocksize = GCS_MIN_BLOCK_SIZE
            self.location = None

    def info(self):
        """ File information about this path """
        return self.details

    def url(self):
        """ HTTP link to this file's data """
        return self.details["mediaLink"]

    def _upload_chunk(self, final=False):
        """Write one part of a multi-block file upload

        Parameters
        ----------
        final: bool
            Complete and commit upload
        """
        while True:
            # shortfall splits blocks bigger than max allowed upload
            data = self.buffer.getvalue()
            head = {}
            l = len(data)

            if (l < GCS_MIN_BLOCK_SIZE) and not final:
                # either flush() was called, but we don't have enough to
                # push, or we split a big upload, and have less left than one
                # block.  If this is the final part, OK to violate those
                # terms.
                return False

            # Select the biggest possible chunk of data to be uploaded
            chunk_length = min(l, GCS_MAX_BLOCK_SIZE)
            chunk = data[:chunk_length]
            if final and self.autocommit and chunk_length == l:
                if l:
                    # last chunk
                    head["Content-Range"] = "bytes %i-%i/%i" % (
                        self.offset,
                        self.offset + chunk_length - 1,
                        self.offset + l,
                    )
                else:
                    # closing when buffer is empty
                    head["Content-Range"] = "bytes */%i" % self.offset
                    data = None
            else:
                head["Content-Range"] = "bytes %i-%i/*" % (
                    self.offset,
                    self.offset + chunk_length - 1,
                )
            head.update(
                {"Content-Type": self.content_type, "Content-Length": str(chunk_length)}
            )
            headers, contents = self.gcsfs.call(
                "POST", self.location, headers=head, data=chunk
            )
            if "Range" in headers:
                end = int(headers["Range"].split("-")[1])
                shortfall = (self.offset + l - 1) - end
                if shortfall:
                    self.checker.update(data[:-shortfall])
                    self.buffer = io.BytesIO(data[-shortfall:])
                    self.buffer.seek(shortfall)
                    self.offset += l - shortfall
                    continue
                else:
                    self.checker.update(data)
            else:
                assert final, "Response looks like upload is over"
                if l:
                    j = json.loads(contents)
                    self.checker.update(data)
                    self.checker.validate_json_response(j)
            # Clear buffer and update offset when all is received
            self.buffer = io.BytesIO()
            self.offset += l
            break
        return True

    def commit(self):
        """If not auto-committing, finalize file"""
        self.autocommit = True
        self._upload_chunk(final=True)

    def _initiate_upload(self):
        """ Create multi-upload """
        self.location = sync(
            self.gcsfs.loop,
            initiate_upload,
            self.gcsfs,
            self.bucket,
            self.key,
            self.content_type,
            self.metadata,
            timeout=self.timeout,
        )

    def discard(self):
        """Cancel in-progress multi-upload

        Should only happen during discarding this write-mode file
        """
        if self.location is None:
            return
        uid = re.findall("upload_id=([^&=?]+)", self.location)
        self.gcsfs.call(
            "DELETE",
            "https://www.googleapis.com/upload/storage/v1/b/%s/o"
            "" % quote_plus(self.bucket),
            params={"uploadType": "resumable", "upload_id": uid},
            json_out=True,
        )

    def _simple_upload(self):
        """One-shot upload, less than 5MB"""
        self.buffer.seek(0)
        data = self.buffer.read()
        sync(
            self.gcsfs.loop,
            simple_upload,
            self.gcsfs,
            self.bucket,
            self.key,
            data,
            self.metadata,
            self.consistency,
            self.content_type,
            timeout=self.timeout,
        )

    def _fetch_range(self, start=None, end=None):
        """Get data from GCS

        start, end : None or integers
            if not both None, fetch only given range
        """
        if start is not None or end is not None:
            start = start or 0
            end = end or 0
            head = {"Range": "bytes=%i-%i" % (start, end - 1)}
        else:
            head = None
        try:
            _, data = self.gcsfs.call("GET", self.details["mediaLink"], headers=head)
            return data
        except RuntimeError as e:
            if "not satisfiable" in str(e):
                return b""
            raise


async def upload_chunk(fs, location, data, offset, size, content_type):
    head = {}
    l = len(data)
    range = "bytes %i-%i/%i" % (offset, offset + l - 1, size)
    head["Content-Range"] = range
    head.update({"Content-Type": content_type, "Content-Length": str(l)})
    headers, txt = await fs._call("POST", location, headers=head, data=data)
    if "Range" in headers:
        end = int(headers["Range"].split("-")[1])
        shortfall = (offset + l - 1) - end
        if shortfall:
            return await upload_chunk(
                fs, location, data[-shortfall:], end, size, content_type
            )
    return json.loads(txt) if txt else None


async def initiate_upload(
    fs, bucket, key, content_type="application/octet-stream", metadata=None
):
    j = {"name": key}
    if metadata:
        j["metadata"] = metadata
    headers, _ = await fs._call(
        method="POST",
        path="https://www.googleapis.com/upload/storage"
        "/v1/b/%s/o" % quote_plus(bucket),
        uploadType="resumable",
        json=j,
        headers={"X-Upload-Content-Type": content_type},
    )
    loc = headers["Location"]
    out = loc[0] if isinstance(loc, list) else loc  # <- for CVR responses
    if len(str(loc)) < 20:
        logger.error("Location failed: %s" % headers)
    return out


async def simple_upload(
    fs,
    bucket,
    key,
    datain,
    metadatain=None,
    consistency=None,
    content_type="application/octet-stream",
):
    checker = get_consistency_checker(consistency)
    path = "https://www.googleapis.com/upload/storage/v1/b/%s/o" % quote_plus(bucket)
    metadata = {"name": key}
    if metadatain is not None:
        metadata["metadata"] = metadatain
    metadata = json.dumps(metadata)
    template = (
        "--==0=="
        "\nContent-Type: application/json; charset=UTF-8"
        "\n\n"
        + metadata
        + "\n--==0=="
        + "\nContent-Type: {0}".format(content_type)
        + "\n\n"
    )

    data = template.encode() + datain + b"\n--==0==--"
    j = await fs._call(
        "POST",
        path,
        uploadType="multipart",
        headers={"Content-Type": 'multipart/related; boundary="==0=="'},
        data=data,
        json_out=True,
    )
    checker.update(datain)
    checker.validate_json_response(j)
