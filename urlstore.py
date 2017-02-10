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
import re
import fcntl
import hashlib
import httplib
import json
from tempfile import mkdtemp
import subprocess

def tostr(s):
    if isinstance(s, str):
        return s
    if isinstance(s, unicode):
        return s.encode('utf8')
    raise ValueError('not a str or unicode', type(s), s)

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
            os.mkdir(newdir, 0775)
            # xxx debug
            # subprocess.call(["ls", "-ld", newdir])

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

# xxx always demand this from the user
default_useragent_string='UrlStore/1.0'

class Headers(dict):

    def __init__(self, dct, typ=None):
        self.type=typ or 'text/plain'
        self.update(dct)

class ErrorResponse(object):
    
    def __init__(self, url, exception, headers=None, msg=None):
        self.url=url
        self.exception=exception
        self.headers=Headers(headers or {})
        self.error_type, self.code=self.classify(exception)

        reason=getattr(self.exception, 'reason', '')
        reason=str(reason) if reason else ''
        self.error_message=filter(None, [reason, msg])


    def __repr__(self):
        return repr(self.exception)

    def __str__(self):
        return str(self.exception)

    def read(self):

        return repr(self.error_message)

    @classmethod
    def classify(cls, e):
        """
        exception --> (error-label, http-status-code)
        """

        # xx complete
        code_to_msg={
            404: "NotFound",
            401: "Unauthorized",
        }

        label,code,msg=None,None,None

        if isinstance(e, socket.timeout):
            label='TimeOut'
        elif hasattr(e,'reason') and isinstance(e.reason, socket.timeout): # xx deprecate?
            label='TimeOut'
        elif hasattr(e,'code'):
            code=e.code
            label=code_to_msg.get(code, "HttpError")
        elif 'Name or service not known' in str(e):
            label='ResolverError'
        else:
            label='HttpError'

        return label,code
        
        

class UrlError(Exception): 
    def __repr__(self):
        return repr(self.args)

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
            sys.stderr.flush()
            time.sleep(shortfall)
        self.host_last_hit[host]=datetime.now()

class UserAgent(object):
    """ urllib2 helper to manage UserAgent header """

    def __init__(self,
                 useragent_string=None, 
                 timeout=3,
                 niceness=30,
                 ):
        self.useragent_string=useragent_string or default_useragent_string
        self.timeout=timeout
        self.nicer=Nicer(niceness)

    def customize_hdr(self, urllib2_request):
        """ modify urllib2.Request instance to be compatible with this ua """
        urllib2_request.add_header('User-Agent', self.useragent_string)
        return urllib2_request

    def get(self, url, **kw):
        """ convenience wrapper around urllib2 """

        self.nicer.be_nice(url)

        req=urllib2.Request(url.encode('utf8'))
        self.customize_hdr(req)
        opt=dict(timeout=self.timeout)
        opt.update(kw)

        try:
            return urllib2.urlopen(req, **opt)
        except urllib2.URLError, e:
            return ErrorResponse(url, e)
        except socket.timeout, e:
            msg=dict(timeout=self.timeout)
            return ErrorResponse(url, e, msg=msg)


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
        self._md=None           # cached md dict

    def __repr__(self):
        return 'Entry("%s")' % (self.id,)

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

    def md(self):
        if not self._md:
            self._md=json.load(file(self.md_path))
            self._md.update(entry=dict(data_path=self.data_path,
                                       md_path=self.md_path,
                                       id=self.id))
        return self._md

    def _content_type(self):
        return self.md().get('headers', {}).get('content-type', '')

    def content_type(self):
        """parsed content type
        example: "text/html; charset=utf-8"  --->  ('text', 'html'), {'charset': 'utf-8'}
        usage: mtype,attrs=entry.content_type()
               assert mtype==('text','html') and attrs.get('charset')=='utf8'
        """
        # xx surprisingly, I could not find a standard lib to do this simply..
        terms=[ term.strip() for term in self._content_type().lower().split(';') ]
        mtype=terms.pop(0)
        mtype_pair=(mtype.split('/')+[None])[:2]
        pairs=[ term.split('=',1) for term in terms ]
        return tuple(mtype_pair), dict([ pair for pair in pairs if len(pair)==2 ])

    def fetch_time(self):
        """fetch time as datetime"""

        return datetime.fromtimestamp(self.md()['timestamp']) #.replace(tzinfo=pytz.UTC)


class Store(object):
    """ simple url-keyed web object store.
    """

    def __init__(self, 
                 store_dir='./x.urlstore', 
                 niceness=30,
                 timeout=5,
                 useragent=None,
                 entry_cls=Entry):
        self.store_dir=store_dir
        self.entry_cls=entry_cls
        self.ua=UserAgent(useragent_string=useragent, niceness=niceness, timeout=timeout)

        mkdir_p(self.store_dir)

    def __repr__(self):
        return 'Store("%s")' % (self.store_dir)

    def url_to_id(self, url):
        # xxx perform generic normalizations.
        # xxx allow custom normalizations.
        return hashlib.md5(url.encode('utf8')).hexdigest()

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
        if id.startswith('_tmp'):
            return os.path.join(self.store_dir, id)
        return os.path.join(self.store_dir, id[:2], id)

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
        sys.stderr.flush()

        return entry

    def _retrieve(self, url):

        id=self.url_to_id(url)
        if self.entry_exists(id):
            return self.entry(id), 'cache-hit'

        return self.fetch(url), 'fetched'

    def fetch(self, url, clear=False):
        """ fetch and store
        """

        id=self.url_to_id(url)

        if not self.entry_exists(id):
            pass
        elif clear:
            rm_rf(self.entry_path(id))
        else:
            raise EntryExists(self.entry_path(id))

        # xx httplib.BadStatusLine
        rsp=self.ua.get(url)
        # rsp could be none if resover fails..
        # xxx slurping up. 
        try:
            data_chunks=[rsp.read()]
        except (httplib.IncompleteRead, socket.timeout), e:
            # xx what to do..
            raise e

        return self.save(id, data_chunks, self.rsp_md(rsp))
        
    def rsp_md(self, rsp):
        """ rsp --> jsonable metadata about the response """
        
        return dict(headers=dict(rsp.headers.items()),
                    status=rsp.code,
                    error_detail=getattr(rsp, 'error_message', None),
                    error_type=getattr(rsp, 'error_type', None),
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
        # 
        # todo: check if entry already exists
        # 
        entry=self.entry(id)
        if os.path.exists(entry.dir_path):
            # xxx should not happen. investigate.
            print >>sys.stderr, 'entry exsits', entry.dir_path
            return entry

        tmp_path=mkdtemp(prefix='_tmp', dir=self.store_dir)
        # temp creats dirs with weired mode (under upstart). fix it here.
        os.chmod(tmp_path, 0775)

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

        # xx should not be here..
        assert not os.path.exists(entry.dir_path), ('entry dir alreay exists', entry.dir_path)
        mkdir_p(entry.dir_path)
        try:
            os.rename(tentry.dir_path, entry.dir_path)
        except OSError, e:
            print >>sys.stderr, 'failed to rename', [tentry.dir_path, entry.dir_path]
            e.args+=(tentry.dir_path, entry.dir_path)
            raise
        
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
    def fetch(store_dir='./x.urlstore', niceness=30, useragent=None):
        """ read urls from stdin.
            print response body to stdout.
            usage:
            $ echo http://localhost/ | urlstore.py fetch
        """

        store=Store(store_dir, niceness=int(niceness), useragent=useragent)

        for url in line_stream():
            try:
                print store.content(url)
                sys.stdout.flush()
            except (UrlError, urllib2.HTTPError), e:
                print >>sys.stderr, e
                sys.stderr.flush()

    @baker.command
    def fetch2(store_dir='./x.urlstore', url_key='url', out_key='url_fetch_md', niceness=30.0, timeout=5.0, useragent=None, verbose=False):
        """ see fetch
        """

        store=Store(store_dir, niceness=float(niceness), timeout=timeout, useragent=useragent)

        for line in sys.stdin.readlines():
            val=json.loads(line)
            url=val[url_key]
            if verbose:
                print >>sys.stderr, url
            try:
                entry,fetch_status=store._retrieve(url)
                error=None
            except Exception, e:
                raise
                print >>sys.stderr,  e, url.encode('utf8')
                sys.stderr.flush()
                entry,fetch_status=None,'error'
                error=dict(exception=repr(e)) # xx client error. what error code?

            if entry:
                val[out_key]=entry.md()
                val['fetch_status']=fetch_status
            else:
                val[out_key]=dict(url=url, status='error', error=error, fetch_status='error')

            print json.dumps(val)
            sys.stdout.flush()

    @baker.command
    def fetch_json(store_dir='./x.urlstore', url_key='url', res_key='res', niceness=30, useragent=None):
        """ { .. url .. } --> { .. url, res .. }
        """

        store=Store(store_dir, niceness=int(niceness), useragent=useragent)

        for line in sys.stdin.readlines():
            val=json.loads(line)
            url=val[url_key]
            try:
                data_json=store.content(url)
            except UrlError, e:
                print >>sys.stderr, e; sys.stderr.flush()
                sys.stderr.flush()
                continue
            data=json.loads(data_json)
            val[res_key]=data
            print json.dumps(val)
            sys.stdout.flush()

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

    @baker.command
    def migrate(store_dir='./x.urlstore'):
        """migrate store layout"""
        store=Store(store_dir)
        from subprocess import Popen, PIPE
        find=Popen(['/usr/bin/find',store.store_dir,'-mindepth','1','-maxdepth','1'],
                  stdout=PIPE)
        for entry_path in line_stream(find.stdout):
            entry_id=os.path.basename(entry_path)
            print entry_path
            new_entry_path=store.entry_path(entry_id)
            mkdir_p(new_entry_path)
            os.rename(entry_path, new_entry_path)
        find.wait()


    try:
        baker.run()
    except IOError, e:
        if e.errno==32:
            pass
        else:
            raise
    except KeyboardInterrupt:
        pass

