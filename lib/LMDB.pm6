use v6;

use NativeCall;

unit module LMDB:ver<0.0.1>;

constant LIB = ('lmdb', v0.0.0);

my enum  EnvFlag is export(:flags) (
    MDB_FIXEDMAP    => 0x01,
    MDB_NOSUBDIR    => 0x4000,
    MDB_NOSYNC	    => 0x10000,
    MDB_RDONLY	    => 0x20000,
    MDB_NOMETASYNC  => 0x40000,
    MDB_WRITEMAP    => 0x80000,
    MDB_MAPASYNC    => 0x100000,
    MDB_NOTLS	    => 0x200000,
    MDB_NOLOCK	    => 0x400000,
    MDB_NORDAHEAD   => 0x800000
);

my enum DbFlag is export(:flags) (
    MDB_REVERSEKEY  => 0x02,
    MDB_DUPSORT	    => 0x04,
    MDB_INTEGERKEY  => 0x08,
    MDB_DUPFIXED    => 0x10,
    MDB_INTEGERDUP  => 0x20,
    MDB_REVERSEDUP  => 0x40,
    MDB_CREATE	    => 0x40000
);

my class MDB-val is repr('CStruct') {
    has size_t		$.mv_size;
    has CArray[uint8]	$.mv_buff;

    submethod BUILD(CArray[uint8] :$mv_buff) {
	$!mv_size = $mv_buff.elems;
	$!mv_buff := $mv_buff;
    }
    method new-from-buf(Blob $b) {
	self.bless(mv_buff => CArray[uint8].new($b.list));
    }
    method new-from-str(Str $str) {
	self.new-from-buf($str.encode('utf8'));
    }
    method Blob() {
	Blob.new($!mv_buff[0 .. ($!mv_size-1)]);
    }
    method Str() {
	self.Blob.decode('utf8');
    }
}

my class MDB-stat is repr('CStruct') {
    has uint32 $.ms_psize;
    has uint32 $.ms_depth;
    has size_t $.ms_branch_pages;
    has size_t $.ms_leaf_pages;
    has size_t $.ms_overflow_pages;
    has size_t $.ms_entries;
}

my class MDB-envinfo is repr('CStruct') {
    has Pointer $.me_mapaddr;
    has size_t  $.me_mapsize;
    has size_t  $.me_last_pgno;
    has size_t  $.me_last_txnid;
    has uint32  $.me_maxreaders;
    has uint32  $.me_numreaders;
}


my sub mdb_strerror(int32) returns Str is native(LIB) { * };

package GLOBAL::X::LMDB {
# Our exceptions
    our class TerminatedTxn is Exception is export {
	method message() { 'Terminated Transaction' }
    }


    our class LowLevel is Exception is export {
#   For errors reported by the lowlevel C library
	has Int $.code;
	has Str $.what;
	submethod BUILD(:$!code, :$!what) { };
	method message() {
	    my $msg;
	    $msg ~= "{$!what}: ";
	    $msg ~= mdb_strerror($!code);
	}
    }
}


my sub mdb_version(Pointer[int32] is rw, Pointer[int32] is rw, Pointer[int32] is rw)
    returns Str is native(LIB) { ... };

our sub version() {
    my $res = mdb_version(
	my Pointer[int32] $mayor .= new,
	my Pointer[int32] $minor .= new,
	my Pointer[int32] $patch .= new
    );
    $res;
}

our class Env {
    class MDB_env is repr('CPointer') {
	sub mdb_env_create(Pointer[Pointer] is rw)
	    returns int32 is native(LIB) { * };
	method new() {
	    my Pointer[Pointer] $p .= new;
	    if mdb_env_create($p) -> $_ {
		X::LMDB::LowLevel.new(code => $_, :what<Can't create>).fail;
	    }
	    nativecast(MDB_env, $p.deref);
	}
    }

    sub mdb_env_close(MDB_env) is native(LIB) { };

    has MDB_env $.env;

    our class Txn { ... };
    has Txn @.txns;

    submethod BUILD(:$!env, :$path, :$size, :$maxdbs, :$maxreaders, :$flags) {
	sub mdb_env_set_mapsize(MDB_env, size_t)
	    returns int32 is native(LIB) { };
	sub mdb_env_set_maxreaders(MDB_env, uint32)
	    returns int32 is native(LIB) { };
	sub mdb_env_set_maxdbs(MDB_env, uint32)
	    returns int32 is native(LIB) { };
	sub mdb_env_open(MDB_env, Str , uint32, int32)
	    returns int32 is native(LIB) { };

	mdb_env_set_mapsize($!env, $size);
	mdb_env_set_maxreaders($!env, $maxreaders) if $maxreaders;
	mdb_env_set_maxdbs($!env, $maxdbs) if $maxdbs;
	if mdb_env_open($!env, $path, $flags || 0, 0o777) -> $_ {
	    X::LMDB::LowLevel.new(code => $_, what => "Can't open $path").fail;
	}
	self;
    }

    method new(
	Str $path,
	Int :$size = 1024 * 1024,
	Int :$maxdbs, Int :$maxreaders, Int :$flags
    ) {
	self.bless(env => MDB_env.new, :$path, :$size, :$maxdbs, :$maxreaders, :$flags);
    }

    method stat(Env:D:) {
	sub mdb_env_stat(MDB_env, MDB-stat)
	    returns int32 is native(LIB) { * };
	mdb_env_stat($!env, my MDB-stat $a .= new);
	Map.new: $a.^attributes.map: { .name.substr(5) => .get_value($a) };
    }

    method info(Env:D:) {
	sub mdb_env_info(MDB_env, MDB-envinfo)
	    returns int32 is native(LIB) { * };
	mdb_env_info($!env, my MDB-envinfo $a .= new);
	Map.new: $a.^attributes.map: { .name.substr(5) => .get_value($a) };
    }

    method get-path(Env:D:) {
	sub mdb_env_get_path(MDB_env, Pointer[Str] is rw)
	    returns int32 is native(LIB) { * };
	mdb_env_get_path($!env, my Pointer[Str] $p .= new);
	$p.deref;
    }

    our class DB { ... };

    our class Txn {
	 class MDB_txn is repr('CPointer') {
	    sub mdb_txn_begin(MDB_env, Pointer, uint64, Pointer[Pointer] is rw)
		returns int32 is native(LIB) { * };
	    method new($env, Pointer $parent, $flags) {
		my Pointer[Pointer] $p .= new;
		if mdb_txn_begin($env, $parent, $flags, $p) -> $code {
		    X::LMDB::LowLevel.new(:$code, :what<Can't create>).fail;
		}
		nativecast(MDB_txn, $p.deref);
	    }
	}

	has Env $!Env;
	has MDB_txn $!txn;

	multi method Bool(::?CLASS:D:) { $!txn.defined };  # Still alive?

	submethod BUILD(:$!Env, :$!txn) { }
	method new(
	    Env $Env,
	    Bool :$subtxn,
	    Int :$flags
	) {
	    my Pointer $parent;
	    self.bless(Env => $Env, txn => MDB_txn.new($Env.env, $parent, $flags || 0));
	}

	method commit(::?CLASS:D: --> True) {
	    sub mdb_txn_commit(MDB_txn)
		returns int32 is native(LIB) { * };

	    X::LMDB::TerminatedTxn.new.fail unless $!txn;
	    if mdb_txn_commit($!txn) -> $code {
		X::LMDB::LowLevel.new(:$code, :what<commit>).fail;
	    }
	    $!txn = Nil;
	}

	method abort(::?CLASS:D: --> True) {
	    sub mdb_txn_abort(MDB_txn)
		returns int32 is native(LIB) { * };

	    X::LMDB::TerminatedTxn.new.fail unless $!txn;
	    if mdb_txn_abort($!txn) -> $code {
		X::LMDB::LowLevel.new(:$code, :what<abort>).fail;
	    }
	    $!txn = Nil;
	}

	method db-open(Str :$name, Int :$flags) {
	    sub mdb_dbi_open(MDB_txn, Str is encoded('utf8'), uint64, Pointer[int32] is rw)
		returns int32 is native(LIB) { * };

	    X::LMDB::TerminatedTxn.new.fail unless $!txn;
	    my Pointer[int32] $rp .= new;
	    if mdb_dbi_open($!txn, $name, $flags || 0, $rp) -> $code {
		X::LMDB::LowLevel.new(:$code, :what<db-open>).fail;
	    }
	    $rp.Int;
	}

	method open(Str :$name, Int :$flags) {
	    DB.new(Txn => self, dbi => self.db-open(:$name, :$flags));
	}

	method opened(Int $dbi) {
	    DB.new(Txn => self, :$dbi);
	}

	sub mdb_put(MDB_txn, uint32, MDB-val, MDB-val, int32)
	    returns int32 is native(LIB) { * };

	multi method put(::?CLASS:D: Int $dbi, Str $key, Buf $val) {
	    X::LMDB::TerminatedTxn.new.fail unless $!txn;
	    if mdb_put($!txn, $dbi,
		       MDB-val.new-from-str($key), MDB-val.new-from-buf($val),
		       0
	    ) -> $code { X::LMDB::LowLevel.new(:$code, :what<put>).fail }
	    $val;
	}
	multi method put(::?CLASS:D: Int $dbi, Str $key, Str $val) {
	    X::LMDB::TerminatedTxn.new.fail unless $!txn;
	    if mdb_put($!txn, $dbi,
		       MDB-val.new-from-str($key), MDB-val.new-from-str($val),
		       0
	    ) -> $code { X::LMDB::LowLevel.new(:$code, :what<put>).fail }
	    $val;
	}

	sub mdb_get(MDB_txn, uint32, MDB-val, MDB-val)
	    returns int32 is native(LIB) { * };

	multi method get(::?CLASS:D: Int $dbi, Str $key) {
	    X::LMDB::TerminatedTxn.new.fail unless $!txn;
	    my $res = MDB-val.new;
	    if mdb_get($!txn, $dbi, MDB-val.new-from-str($key), $res) -> $code {
		X::LMDB::LowLevel.new(:$code, :what<get>).fail;
	    }
	    $res;
	}
	multi method get(::?CLASS:D: Int $dbi, Str $key, Any $val is rw) {
	    X::LMDB::TerminatedTxn.new.fail unless $!txn;
	    my $res = MDB-val.new;
	    if mdb_get($!txn, $dbi, MDB-val.new-from-str($key), $res) -> $code {
		X::LMDB::LowLevel.new(:$code, :what<get>).fail;
	    }
	    $val = $res;
	}

	method del(::?CLASS:D: Int $dbi, Str $key, Any $val = Nil --> True) {
	    sub mdb_del(MDB_txn, uint32, MDB-val, MDB-val)
		returns int32 is native(LIB) { * };
	    X::LMDB::TerminatedTxn.new.fail unless $!txn;
	    my $match = $val.defined ?? (
		$val ~~ Buf ?? MDB-val.new-from-buf($val) !! MDB-val.new-from-str(~$val)
	    ) !! MDB-val.new;
	    if mdb_del($!txn, $dbi, MDB-val.new-from-str($key), $match) -> $code {
		X::LMDB::LowLevel.new(:$code, :what<del>).fail;
	    }
	}

	method stat(::?CLASS:D: Int $dbi) {
	    sub mdb_stat(MDB_txn, uint32, MDB-stat)
		returns int32 is native(LIB) { * };

	    X::LMDB::TerminatedTxn.new.fail unless $!txn;
	    if mdb_stat($!txn, $dbi, my MDB-stat $a .= new) -> $code {
		X::LMDB::LowLevel.new(:$code, :what<stat>).fail;
	    }
	    Map.new: $a.^attributes.map: { .name.substr(5) => .get_value($a) };
	}

    }

    method begin-txn(Int :$flags, Bool :$subtxn) {
	Txn.new(self, :$subtxn, :$flags);
    }

    # A high level class for encapsulates a Txn and a dbi
    class DB does Associative {
	has Txn $.Txn;
	has Int $.dbi;

	multi method AT-KEY(::?CLASS:D: $key) {
	    my \SELF = self;
	    Proxy.new(
		FETCH => method () { SELF.Txn.get(SELF.dbi, $key) },
		STORE => method ($val) { SELF.Txn.put(SELF.dbi, $key, $val) }
	    )
	}

	multi method EXISTS-KEY(::?CLASS:D: $key) {
	    $!Txn.get($!dbi, $key).defined;
	}

	multi method DELETE-KEY(::?CLASS:D: $key) {
	    $!Txn.del($!dbi, $key)
	}

    }
}

