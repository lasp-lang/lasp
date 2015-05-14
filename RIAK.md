## Building Riak with Lasp

Clone a fork of riak, which integrates cmeiklejohn/lasp and
cmeiklejohn/riak_kv.

```
$ git clone git://github.com/cmeiklejohn/riak.git
$ git checkout lasp
```

Compile riak.

```
$ make
```

Execute the demo script. This demo script will do the following:

* Start Riak
* Write about 1000 keys to Riak
* Execute the Lasp-driven key list query.
* Execute the Lasp-driven 2i query.
* Stop Riak.

```
$ scripts/2i.sh
```
