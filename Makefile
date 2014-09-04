
all:

clean:
	rm -f *.pyc README

purge:
	rm -fr x.urlstore

README:
	python -c "import urlstore; print urlstore.__doc__" > $@.tmp
	mv -f $@.tmp $@

#### examples
url=http://github.com/api/v2/json/repos/search/url+store
url=http://localhost:8000/
fetch:
	echo $(url) | python urlstore.py fetch --useragent="$(useragent)"


dump:
	echo $(url) | python urlstore.py dump
