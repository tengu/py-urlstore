
all:

clean:
	rm -f *.pyc README

purge:
	rm -fr x.urlstore

# http://github.com/api/v2/json/repos/show/joyent/node/watchers
url=http://github.com/api/v2/json/repos/search/url+store
url=http://localhost:8000/
t:
	rm -fr x.urlstore/*
	echo $(url) \
	| python urlstore.py fetch \
	> /dev/null

d:
	echo $(url) \
	| python urlstore.py dump


README:
	python -c "import urlstore; print urlstore.__doc__" > $@.tmp
	mv -f $@.tmp $@

m:
	python urlstore.py migrate
