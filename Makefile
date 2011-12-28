
all:

clean:
	rm -f *.pyc README

purge:
	rm -fr x.urlstore

# http://github.com/api/v2/json/repos/show/joyent/node/watchers
url=http://github.com/api/v2/json/repos/search/url+store
t:
	echo $(url) \
	| python urlstore.py fetch \
	| json-cut repositories/[]/name

d:
	echo $(url) \
	| python urlstore.py dump


README:
	python -c "import urlstore; print urlstore.__doc__" > $@.tmp
	mv -f $@.tmp $@
