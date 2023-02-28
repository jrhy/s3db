
The S3DB extension should be used wherever possible, like in the `sqlite` CLI,
or loading into the JVM. But if you're using Go, keep reading.

= Caveats =
Go programs must be built with `-linkshared` in order to use this extension.
On Mac, where `-linkshared` is not available:

```
go test -v -linkshared
-linkshared not supported on darwin/amd64
```

the `../mattn` module should be used instead. Attempts to use the shared module
in a Go program when -linkshared is missed will result in crashes like:

```
fatal error: bad sweepgen in refill

goroutine 21 [running, locked to thread]:
runtime.throw({0x8e6c74df?, 0xc000165198?})
	/home/foo/sdk/go1.18/src/runtime/panic.go:992 +0x71 fp=0xc000164f20 sp=0xc000164ef0 pc=0x8e035751
runtime.(*mcache).refill(0x542e1d8, 0xb)
	/home/foo/sdk/go1.18/src/runtime/mcache.go:156 +0x1eb fp=0xc000164f58 sp=0xc000164f20 pc=0x8e018f0b
runtime.(*mcache).nextFree(0x542e1d8, 0xb)
	/home/foo/sdk/go1.18/src/runtime/malloc.go:886 +0x85 fp=0xc000164fa0 sp=0xc000164f58 pc=0x8e00ee45
runtime.mallocgc(0x2b, 0x0, 0x0)
	/home/foo/sdk/go1.18/src/runtime/malloc.go:1085 +0x4e5 fp=0xc000165018 sp=0xc000164fa0 pc=0x8e00f4c5
runtime.rawstring(0x2b)
	/home/foo/sdk/go1.18/src/runtime/string.go:273 +0x37 fp=0xc000165068 sp=0xc000165018 pc=0x8e0501f7
runtime.rawstringtmp(0x542e1d8?, 0x10?)
```

