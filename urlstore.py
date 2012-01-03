#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""urlstore: cached url fetch as unix pipe

The command takes a stream of urls from stdin and outputs contents:
$ cat urls | urlstore.py fetch | process-the-output

When pointed to a JSON api, urlstore acts as a unix pipe to convert 
lines of url to the json output stream:

echo http://github.com/api/v2/json/repos/search/url+store \\
	| python urlstore.py fetch \\
	| json-cut repositories/[]/name
url_store
url-store
urlStore

The main usage is to avoid hitting a site with same url while 
investigating the api.  After the first invocation, the content 
is fetched from a local store, so you can rapidly iterate like you 
are grepping a local file.

API
Url that returns JSON can be converted to python data by store.data(url):

>>> from urlstore import Store
>>> store=Store()
    # convert url to data (assuming that the url returns json)
>>> for repo in store.data('http://github.com/api/v2/json/repos/search/url+store')['repositories']:
...     print repo['name']
...
url_store
url-store
urlStore

Features:
* builtin niceness guard so you don't get banned from the site.
* simple filesystem-based store: $stor_dir/$entry_id/{content,md}
  - entry_id is the md5 of url.
  - content contains the response body
  - md contains headers and other metadata as json

TODO:
* add cache policy
* make sure fetch start (not end time) time is used for niceness 
* heed throttling warning headers from the server.
* option to dump headers to stderr

"""
import sys,os
import fcntl
import hashlib
import json
from tempfile import mkdtemp

#### util
def line_stream(filehandle=sys.stdin):
    """ non-buffered, chomped line stream. """ 
    while True:
        line=filehandle.readline()
        if line in set([None, '']):
            raise StopIteration
        yield line.strip('\n')

from datetime import datetime
from time import mktime
def unixtimestamp():
    return mktime(datetime.now().timetuple())

def mkdir_p(newdir):
    """
    from http://code.activestate.com/recipes/82465/
    works the way a good mkdir should :)
        - already exists, silently complete
        - regular file in the way, raise an exception
        - parent directory(ies) does not exist, make them as well
    """
    if os.path.isdir(newdir):
        pass
    elif os.path.isfile(newdir):
        raise OSError("a file with the same name as the desired " \
                      "dir, '%s', already exists." % newdir)
    else:
        head, tail = os.path.split(newdir)
        if head and not os.path.isdir(head):
            mkdir_p(head)
        if tail:
            os.mkdir(newdir)

def rm_rf(p):
    """ rm -fr use at your own risk """
    if not os.path.exists(p):
        pass
    elif os.path.isdir(p):
        for ep in (os.path.join(p,e) for e in os.listdir(p)):
            rm_rf(ep)
        os.rmdir(p)
    else:                       # isfile or islink
        os.unlink(p)

#### ua
import time
import urllib2
import socket
from urlparse import urlparse

default_useragent_string='UrlStore/1.0'

class UrlError(Exception): 
    def __repr__(self):
        return str(self)
    def __str__(self):
        try:
            attrs=dict(self.args)
            return "%s	%s" % (attrs['error'], attrs['url'])
        except:
            return str(self.args)

class UrlTimeout(UrlError): pass
class UrlNotFound(UrlError): pass
class UrlUnauthorized(UrlError): pass

class Nicer(object):
    """ manage niceness constraints
    """

    def __init__(self, niceness):
        self.niceness=niceness
        self.host_last_hit={}

    def be_nice(self, url):
        """ sleep as necessary to comply with niceness
        """
        u=urlparse(url)
        host=u.hostname
        last_hit=self.host_last_hit.get(host, datetime.min)
        elapsed=datetime.now()-last_hit
        shortfall=self.niceness-elapsed.seconds
        if shortfall>0:
            print >>sys.stderr, 'sleeping', shortfall, host
            time.sleep(shortfall)
        self.host_last_hit[host]=datetime.now()

class UserAgent(object):
    """ urllib2 helper to manage UserAgent header """

    def __init__(self,
                 useragent_string=default_useragent_string, 
                 timeout=60,
                 niceness=3,
                 ):
        self.useragent_string=useragent_string
        self.timeout=60         # default timeout
        self.nicer=Nicer(niceness)

    def customize_hdr(self, urllib2_request):
        """ modify urllib2.Request instance to be compatible with this ua """
        urllib2_request.add_header('User-Agent', self.useragent_string)
        return urllib2_request

    def get(self, url, **kw):
        """ convenience wrapper around urllib2 """

        self.nicer.be_nice(url)

        req=urllib2.Request(url)
        self.customize_hdr(req)
        opt=dict(timeout=self.timeout)
        opt.update(kw)

        try:
            r=urllib2.urlopen(req, **opt)

        except urllib2.URLError, e:
            # because the exceptions that urllib2 throw is unwieldy, 
            # wrap them in our own hierarchy for ease of catching.
            if hasattr(e,'reason') and isinstance(e.reason, socket.timeout):
                raise UrlTimeout(('timeout', opt.get('timeout')), ('url', url), 
                                 ('error', e))

            elif hasattr(e,'code') and e.code==404:
                raise UrlNotFound(('url', url), ('error', e))
                
            elif hasattr(e,'code') and e.code==401:
                raise UrlUnauthorized(('url', url), ('error', e))
                
            else:
                e.args+=(url,)
                raise

        return r

#### store entry
class EntryExists(Exception):
    pass

class Entry(object):
    """ Store Entry """

    def __init__(self, store, id):
        self.store=store
        self.id=id
        self.dir_path=self.store.entry_path(id)
        self.data_path=os.path.join(self.dir_path, 'content')
        self.md_path=os.path.join(self.dir_path, 'md')
        self.lock_path=os.path.join(self.dir_path, 'lock')

    def __repr__(self):
        return 'entry("%s")' % (self.id,)

    def exists(self):
        """ am I persisted """
        return self.store.entry_exists(self.id)

    def lock(self):
        """ take out an advisory lock to keep my temp dir from cleaned up
        """
        # touch and flock
        f=file(self.lock_path, 'w')
        f.write(' ') # may be write pid..
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)

    def unlock(self):
        """ undo self.lock() 
            if dir_path is a temp path, it can be cleaned up.
        """
        f=file(self.lock_path, 'w')
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)

class Store(object):
    """ simple url-keyed web object store.
    """

    def __init__(self, 
                 store_dir='./x.urlstore', 
                 niceness=3,
                 entry_cls=Entry):
        self.store_dir=store_dir
        self.entry_cls=entry_cls
        self.ua=UserAgent()

        mkdir_p(self.store_dir)

    def url_to_id(self, url):
        # xxx perform generic normalizations.
        # xxx allow custom normalizations.
        return hashlib.md5(url).hexdigest()

    def ids(self):
        """ enumerate stored ids.
            loads into memory. 
            todo: use generator..
        """
        sd=self.store_dir
        return [ os.path.join(sd, f) for f in os.listdir(sd) ]

    def entry_exists(self, id):
        return os.path.exists(self.entry_path(id))

    def entry_path(self, id):
        """ id --> entry-path """
        return os.path.join(self.store_dir, id)

    def entry(self, id, *args, **kw):
        """ create a volatile/non-persisted entry """
        return self.entry_cls(self, id, *args, **kw)

    def entries(self):
        """ traverse saved entris """
        for id in self.ids():
            yield self.lookup_by_id(id)

    def lookup_by_id(self, id):
        """ id --> entry """

        entry=self.entry(id)
        if entry.exists():
            return entry
        return None

    def retrieve(self, url):
        """ return Entry fetching as necessary.
        """

        entry, status=self._retrieve(url)
        print >>sys.stderr, "%s\t%s" % (status, url) # xx logging

        return entry

    def _retrieve(self, url):

        id=self.url_to_id(url)
        if self.entry_exists(id):
            return self.entry(id), 'cache-hit'

        return self.fetch(url), 'fetched'

    def fetch(self, url, clobber=False):
        """ fetch and store
        """

        id=self.url_to_id(url)

        if not self.entry_exists(id):
            pass
        elif clobber:
            rm_rf(self.entry_path(id))
        else:
            raise EntryExists(self.entry_path(id))

        rsp=self.ua.get(url)

        # xxx slurping up. 
        data_chunks=[rsp.read()]

        return self.save(id, data_chunks, self.rsp_md(rsp))
        
    def rsp_md(self, rsp):
        """ rsp --> jsonable metadata about the response """

        return dict(headers=dict(rsp.headers.items()),
                    status=rsp.code,
                    type=rsp.headers.type,
                    timestamp=int(unixtimestamp()),
                    url=rsp.url)

    def save(self, id, data_chunks, metadata):
        """ save data to fs.
            -data_chunks stream of data chunks that comprise the content.
            -metadata object hierarchy.

            layout: 
                {store_dir}/{id}/content|metadata
        """

        tmp_path=mkdtemp(prefix='_tmp', dir=self.store_dir)
        tmp_id=os.path.basename(tmp_path)
        tentry=self.entry(tmp_id)
        # a temp directory that's locked ($dir/lock exists and flocked)
        # should not be cleaned up.
        tentry.lock()

        entry=self._save(tentry, id, data_chunks, metadata)

        # xxx unlock

        return entry

    def _save(self, tentry, id, data_chunks, metadata):
        """ see save() """

        # save data
        data_file=file(tentry.data_path, 'w')
        for chunk in data_chunks:
            data_file.write(chunk)
        data_file.close()
        
        # serialize, save md
        md_file=file(tentry.md_path, 'w')
        md_file.write(json.dumps(metadata, indent=4))

        # mv {id}.tmp {id}
        entry=self.entry(id)
        os.rename(tentry.dir_path, entry.dir_path)
        
        return entry

    #### convenience methods
    def content(self, url):
        """ url --> content """
        entry=self.retrieve(url)
        return file(entry.data_path).read()

    def data(self, url):
        """ url --> python-data if the type is json """
        return json.loads(self.content(url))

if __name__=='__main__':

    import baker

    @baker.command
    def fetch(store_dir='./x.urlstore'):
        """ read urls from stdin.
            print response body to stdout.
            usage:
            $ echo http://localhost/ | urlstore.py fetch
        """

        store=Store(store_dir)

        for url in line_stream():
            try:
                print store.content(url)
            except UrlError, e:
                print >>sys.stderr, e

    @baker.command
    def fetch_json(store_dir='./x.urlstore', request_url_key='_request_url'):
        """like fetch but request url is added to the json output.
        """

        store=Store(store_dir)

        for url in line_stream():
            try:
                data_json=store.content(url)
            except UrlError, e:
                print >>sys.stderr, e
                continue
            data=json.loads(data_json)
            data[request_url_key]=url
            print json.dumps(data)

    @baker.command
    def dump(store_dir='./x.urlstore'):
        """ for each url fed to stdin, dump store entry info.
            usage:
            $ echo http://localhost/ | urlstore.py dump
        """

        store=Store(store_dir)

        for url in line_stream():
            entry=store.retrieve(url)
            print entry
            print entry.data_path
            print entry.md_path

    baker.run()
