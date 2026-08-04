# Attaching Per-Write Metadata to RocksDB Puts (Prototype Design)

## Goal

For every `Put` in a transaction, record:

- a **list of keys the written value depends on** (`dep_keys`)
- a **type flag** marking what kind of op the write is within the transaction (`type`)

...and be able to set both from the Java `org.rocksdb` API.

## Why we need it (current state)

The repair path in `utilities/transactions/optimistic_transaction.cc:174-266` iterates
over tracked write keys and hardcodes `parsed_value + 10`, because it has no way to know:

- which conflicted read key a given write's value was derived from — it just grabs
  `conflicted_read_keys.begin()` (line 213-215)
- what kind of op the write was, so DepositChecking / WriteCheck / TransactSaving are
  all treated identically (see comment at line 207)
- how to handle ops with two dependent keys — the open `TODO` about Amalgamate at line 208

The metadata's only consumer is C++ at **commit time**. It does not need to be durable
and does not need to reach the WAL. That observation drives the whole design.

## Recommended approach: side map on the transaction object

Do **not** change the signature of `Put()`. There are 6+ `put` JNI variants
(`put__J_3BII_3BIIJZ`, `putDirect`, `put__J_3_3BI_3_3BIJZ`, `putUntracked`, ...) and
overloading them all would be a large amount of boilerplate for a prototype.

Instead, add one setter that annotates a write to a given key, stored in a map hanging
off the transaction.

### 1. `utilities/transactions/transaction_base.h` — in `TransactionBaseImpl`

```cpp
struct WriteMeta {
  int32_t type = 0;                     // op-type flag, app-defined
  std::vector<std::string> dep_keys;    // keys this write's value depends on
};

void SetWriteMeta(ColumnFamilyHandle* cfh, const Slice& key,
                  int32_t type, std::vector<std::string> deps) {
  uint32_t cf = cfh ? cfh->GetID() : 0;
  write_meta_[{cf, key.ToString()}] = WriteMeta{type, std::move(deps)};
}

const WriteMeta* GetWriteMeta(uint32_t cf, const std::string& key) const {
  auto it = write_meta_.find({cf, key});
  return it == write_meta_.end() ? nullptr : &it->second;
}

protected:
  std::map<std::pair<uint32_t, std::string>, WriteMeta> write_meta_;
```

Keying on `(cf_id, key)` matches how the repair loop already iterates
(column family iterator, then key iterator over `tracked_locks_`).

**Important:** clear `write_meta_` in `TransactionBaseImpl::Clear()` alongside the other
per-transaction state. The SmallBank benchmark reuses transaction objects, so stale
metadata would otherwise leak across `Commit()` boundaries.

### 2. Consume it in the repair loop

Replaces the "first conflicted read key" guesswork:

```cpp
const WriteMeta* meta = GetWriteMeta(cf, key_bytes);
if (meta == nullptr) continue;              // unannotated write, leave alone

for (const auto& dep : meta->dep_keys) {
  // look dep up in conflicted_read_keys -- now precise, not begin()
}

switch (meta->type) {
  case kDeposit:     /* ... */ break;
  case kAmalgamate:  /* uses two dep_keys */ break;
  // ...
}
```

This is what unblocks the Amalgamate `TODO` at line 208.

### 3. JNI — one new function in `java/rocksjni/transaction.cc`

Model the key extraction on the existing `Java_org_rocksdb_Transaction_put__J_3BII_3BIIJ`
body; deps arrive as a `jobjectArray` of `byte[]`. A plain loop over `GetArrayLength` +
`GetByteArrayElements` is adequate for a prototype.

```cpp
void Java_org_rocksdb_Transaction_setWriteMeta(
    JNIEnv* env, jclass, jlong jhandle, jbyteArray jkey, jint jkey_len,
    jint jtype, jobjectArray jdeps, jlong jcf_handle);
```

### 4. Java — `java/src/main/java/org/rocksdb/Transaction.java`

```java
public void setWriteMeta(ColumnFamilyHandle cf, byte[] key, int type, byte[][] deps) {
  assert isOwningHandle();
  setWriteMeta(nativeHandle_, key, key.length, type, deps, cf.nativeHandle_);
}

// convenience: annotate + write in one call
public void putWithMeta(ColumnFamilyHandle cf, byte[] key, byte[] value,
                        int type, byte[][] deps) throws RocksDBException {
  setWriteMeta(cf, key, type, deps);
  put(cf, key, value);
}

private native void setWriteMeta(long handle, byte[] key, int keyLen,
                                 int type, byte[][] deps, long cfHandle);
```

Call site in the SmallBank benchmark:

```java
txn.putWithMeta(checkingCF, acctKey, newBal, OP_DEPOSIT, new byte[][]{acctKey});
```

## Rejected alternatives

| Approach | Why not |
| --- | --- |
| Encode metadata in the value (length-prefixed header) | Zero C++ changes, but every reader must strip it, the repair code has to re-parse it, and it changes the on-disk value format for anything else reading the DB. |
| Custom `WriteBatch` record type / `PutLogData` | The "proper" route if metadata must survive into the WAL, but requires recovery-path and format plumbing. Unnecessary — the metadata is only read at commit time. |
| Overload every `Put` variant | Large JNI surface (6+ symbols x direct/indirect/cf variants) for no added capability over the side map. |

## Files touched

- `utilities/transactions/transaction_base.h` — `WriteMeta`, `write_meta_`, accessors
- `utilities/transactions/transaction_base.cc` — clear `write_meta_` in `Clear()`
- `utilities/transactions/optimistic_transaction.cc` — use `dep_keys` / `type` in repair loop (ABORT_MODE=3)
- `java/rocksjni/transaction.cc` — `Java_org_rocksdb_Transaction_setWriteMeta`
- `java/src/main/java/org/rocksdb/Transaction.java` — `setWriteMeta`, `putWithMeta`
