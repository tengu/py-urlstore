from setuptools import setup
#from distutils.core import setup

def doc():
    import urlstore
    return urlstore.__doc__
    
setup(
    name = "urlstore",
    version = "0.0.1",
    py_modules = ["urlstore"],
    scripts = ["urlstore.py"],
    license = "LGPL",
    description = "cached url fetching as unix pipe command: cat urls | urlstore.py | ...",
    author = "tengu",
    author_email = "karasuyamatengu@gmail.com",
    url = "https://github.com/tengu/py-urlstore",
    # http://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers = [
        "License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)",
        "Environment :: Console",
        "Programming Language :: Python",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Internet :: WWW/HTTP", 
        "Topic :: Text Processing :: Filters", 
        "Topic :: Utilities", 
        "Topic :: Software Development :: Libraries :: Python Modules",
        ],
    #long_description = doc(),
    long_description="""
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
""",
    )
