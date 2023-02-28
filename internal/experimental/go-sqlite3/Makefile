all: vet test-loadable test-mattn

vet:
	go vet -tags sqlite_vtable

test-mattn:
	VTABLE_TEST_LOADABLE_EXT=false \
	go test -v -tags sqlite_vtable

test-loadable:
	if [ $$(uname -o) != "Darwin" ] ; then \
		VTABLE_TEST_LOADABLE_EXT=true \
		AWS_REGION=dummy \
		AWS_ACCESS_KEY_ID=dummy \
		AWS_SECRET_ACCESS_KEY=dummy \
		go test -v -linkshared -tags sqlite_vtable ; \
	else \
		echo skipping loadable test on MacOS due to lack of support for -linkshared ; \
	fi

