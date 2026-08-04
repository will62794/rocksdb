//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/transactions/optimistic_transaction.h"

#include <cstdint>
#include <string>
#include <iostream>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "util/cast_util.h"
#include "util/defer.h"
#include "util/string_util.h"
#include "utilities/transactions/lock/point/point_lock_tracker.h"
#include "utilities/transactions/optimistic_transaction_db_impl.h"
#include "utilities/transactions/transaction_util.h"
#include "logging/logging.h"


namespace ROCKSDB_NAMESPACE {

struct WriteOptions;

OptimisticTransaction::OptimisticTransaction(
    OptimisticTransactionDB* txn_db, const WriteOptions& write_options,
    const OptimisticTransactionOptions& txn_options)
    : TransactionBaseImpl(txn_db->GetBaseDB(), write_options,
                          PointLockTrackerFactory::Get()),
      txn_db_(txn_db) {
  Initialize(txn_options);
}

void OptimisticTransaction::Initialize(
    const OptimisticTransactionOptions& txn_options) {
  if (txn_options.set_snapshot) {
    SetSnapshot();
  }
}

void OptimisticTransaction::Reinitialize(
    OptimisticTransactionDB* txn_db, const WriteOptions& write_options,
    const OptimisticTransactionOptions& txn_options) {
  TransactionBaseImpl::Reinitialize(txn_db->GetBaseDB(), write_options);
  Initialize(txn_options);
}

OptimisticTransaction::~OptimisticTransaction() = default;

void OptimisticTransaction::Clear() { TransactionBaseImpl::Clear(); }

Status OptimisticTransaction::Prepare() {
  return Status::InvalidArgument(
      "Two phase commit not supported for optimistic transactions.");
}

Status OptimisticTransaction::Commit() {
  auto txn_db_impl = static_cast_with_check<OptimisticTransactionDBImpl,
                                            OptimisticTransactionDB>(txn_db_);
  assert(txn_db_impl);

  ROCKS_LOG_DETAILS(dbimpl_->immutable_db_options().info_log, "Transaction Commit");
//   std::cout << "Transaction Commit" << std::endl;
  
  switch (txn_db_impl->GetValidatePolicy()) {
    case OccValidationPolicy::kValidateParallel:
      return CommitWithParallelValidate();
    case OccValidationPolicy::kValidateSerial:
      return CommitWithSerialValidate();
    default:
      assert(0);
  }
  // unreachable, just void compiler complain
  return Status::OK();
}

Status OptimisticTransaction::CommitWithSerialValidate() {
  // Set up callback which will call CheckTransactionForConflicts() to
  // check whether this transaction is safe to be committed.
  OptimisticTransactionCallback callback(this);

  DBImpl* db_impl = static_cast_with_check<DBImpl>(db_->GetRootDB());

  Status s = db_impl->WriteWithCallback(
      write_options_, GetWriteBatch()->GetWriteBatch(), &callback);

  if (s.ok()) {
    Clear();
  }

  return s;
}

Status OptimisticTransaction::CommitWithParallelValidate() {
  auto txn_db_impl = static_cast_with_check<OptimisticTransactionDBImpl,
                                            OptimisticTransactionDB>(txn_db_);
  assert(txn_db_impl);
  DBImpl* db_impl = static_cast_with_check<DBImpl>(db_->GetRootDB());
  assert(db_impl);
  std::set<port::Mutex*> lk_ptrs;
  std::unique_ptr<LockTracker::ColumnFamilyIterator> cf_it(
      tracked_locks_->GetColumnFamilyIterator());
  assert(cf_it != nullptr);
  while (cf_it->HasNext()) {
    ColumnFamilyId cf = cf_it->Next();

    // To avoid the same key(s) contending across CFs or DBs, seed the
    // hash independently.
    uint64_t seed = reinterpret_cast<uintptr_t>(db_impl) +
                    uint64_t{0xb83c07fbc6ced699} /*random prime*/ * cf;

    std::unique_ptr<LockTracker::KeyIterator> key_it(
        tracked_locks_->GetKeyIterator(cf));
    assert(key_it != nullptr);
    while (key_it->HasNext()) {
      auto lock_bucket_ptr = &txn_db_impl->GetLockBucket(key_it->Next(), seed);
      TEST_SYNC_POINT_CALLBACK(
          "OptimisticTransaction::CommitWithParallelValidate::lock_bucket_ptr",
          lock_bucket_ptr);
      lk_ptrs.insert(lock_bucket_ptr);
    }
  }
  // NOTE: in a single txn, all bucket-locks are taken in ascending order.
  // In this way, txns from different threads all obey this rule so that
  // deadlock can be avoided.
  for (auto v : lk_ptrs) {
    // WART: if an exception is thrown during a Lock(), previously locked will
    // not be Unlock()ed. But a vector of MutexLock is likely inefficient.
    v->Lock();
  }
  Defer unlocks([&]() {
    for (auto v : lk_ptrs) {
      v->Unlock();
    }
  });

  // If incoming write conflict exists, then we could return set of keys in invalidated read set.
  std::set<std::pair<std::string, std::string>> conflicted_read_keys;
  Status s = TransactionUtil::CheckKeysForConflicts(db_impl, *tracked_locks_,
                                                    true /* cache_only */,
                                                    conflicted_read_keys);

//   std::cout << "Conflicted read keys: " << conflicted_read_keys.size() << std::endl;
//   for(const auto& key : conflicted_read_keys){
//       std::cout << "  " << key << std::endl;
//   }

    int isolation_abort_mode = 0;

    // INSERT_YOUR_CODE
    const char* abort_mode_env = std::getenv("ABORT_MODE");
    if (abort_mode_env != nullptr) {
    isolation_abort_mode = std::atoi(abort_mode_env);
    }

    int32_t new_value = 0;

  // For each conflicted read key, get its latest value.
  // We now have the latest read value for each key in conflicted read key set.

  // Along with an optimistic transaction, we can store a set 
  // "functions" that are update expressions for each key that is modified.

  // For each key write in this transaction, we need to re-compute its output based on the updated read values.

  // for each updated key, re-compute its output based on the new value of any keys it read and were updated.
  int32_t parsed_value = -1;
  Status sa2;

  // REPAIR MODE case.
  if(isolation_abort_mode == 3){
    std::unique_ptr<LockTracker::ColumnFamilyIterator> cf_it2(
        tracked_locks_->GetColumnFamilyIterator());
        assert(cf_it2 != nullptr);

        // Iterator over each column family.
        while (cf_it2->HasNext()) {
            ColumnFamilyId cf = cf_it2->Next();
            ColumnFamilyHandle* cfh = db_impl->GetColumnFamilyHandle(cf);

            char new_val_bytes[4];

            // std::cout << "Column Family: " << cf << std::endl;
            std::unique_ptr<LockTracker::KeyIterator> key_it2(
                tracked_locks_->GetKeyIterator(cf));
            assert(key_it2 != nullptr);

            // Iterate over all keys written in this transaction.
            while (key_it2->HasNext()) {

                    std::string key_bytes = key_it2->Next();

                    // We are only concerned with checking (and repairing) modifications to writes.
                    PointLockStatus status = tracked_locks_->GetPointLockStatus(cf, key_bytes);
                    if(!status.had_write){
                        continue;
                    }

                    // Metadata tagged on this write from Java via
                    // Transaction.setWriteMeta()/putWithMeta(). Null if untagged.
                    // Nothing below uses it yet -- this is just the access point.
                    const WriteMeta* meta = GetWriteMeta(cf, key_bytes);
                    int op_type = -1;
                    // The set of keys this write (read) depends on. 
                    std::vector<WriteMeta::DepKey> dep_keys = {};
                    if (meta != nullptr) {
                        op_type = meta->type;
                        // Each dep is a (column family id, key) pair.
                        dep_keys = meta->dep_keys;
                        // std::cout << "type=" << op_type << " ndeps=" << dep_keys.size() << std::endl;
                        // for(const auto& dep : dep_keys){
                        //     std::cout << "  dep cf=" << dep.column_family_id
                        //               << " dep_key=" << dep.key << std::endl;
                        // }
                    }

                    // INSERT_YOUR_CODE
                    // Print out the key and its read dependencies for debugging
                    std::cout << "[Repair] Key (op_type=" << op_type << "): ";
                    for (size_t i = 0; i < key_bytes.size(); ++i) {
                        printf("%02x", static_cast<unsigned char>(key_bytes[i]));
                    }
                    std::cout << " depends on keys: ";
                    for (const auto& dep : dep_keys) {
                        std::cout << "[cf:" << dep.column_family_id << " key:";
                        for (size_t k = 0; k < dep.key.size(); ++k) {
                            printf("%02x", static_cast<unsigned char>(dep.key[k]));
                        }
                        std::cout << "] ";
                    }
                    std::cout << std::endl;
               
         

                    Slice keyx(key_bytes);
                    // std::cout << "Key: " << keyx.ToString() << std::endl;
                    // Encode the 32-bit integer 0 as raw 4 bytes, big-endian, and store as string.
                    char val_bytes[4] = {0, 0, 0, 0};
                    Slice value(val_bytes, 4);
                    // Log the conflicted read keys for debugging purposes
                    if (!conflicted_read_keys.empty()) {
                        // std::cout << "[Log] Conflicted read keys (" << conflicted_read_keys.size() << "):" << std::endl;
                        // for (const auto& key : conflicted_read_keys) {
                            // std::cout << "  " << key << std::endl;
                        // }
                    }

                    // For both DepositChecking, WriteCheck, and TransactSaving, repair will be the same, even though on different column families.
                    // TODO: Amalgamate is more involved, and requires associating the update with the correct two dependent keys.
                    
                    // Get the first conflicted read key, if any exist
                    // std::string conflicted_read_key;
                    std::string conflicted_read_value;

                    // int32_t sum = 0;
                    // Store dep_keys values.
                    std::vector<int32_t> dep_values = {};
                    
                    // branch on op_type
                    switch(op_type){
                        case 0:
                            // Balance
                            break;
                        case 1:
                            // WriteCheck
                            {
                                // DepositChecking
                                // Assume all deposit increments are in values of 10 for right now.
    
                                // For each dep key, get is value if it exists in the set of conflicted read keys.
                                for(const auto& dep : dep_keys){
                                    // dep_key_value = dep.value;
    
                                    // If this conflicted read key matches the dep key, get its value.
                                    // Find conflicted read key that matches the dep key.
    
                                    // Iterate over conflicted_read_keys.
                                    for(const auto& pair : conflicted_read_keys){
                                        if(pair.first == dep.key){
                                            conflicted_read_value = pair.second;
                                            assert(conflicted_read_value.size() == 4);
                                            parsed_value = 
                                                ((static_cast<uint8_t>(conflicted_read_value[0]) << 24) |
                                                (static_cast<uint8_t>(conflicted_read_value[1]) << 16) |
                                                (static_cast<uint8_t>(conflicted_read_value[2]) << 8)  |
                                                (static_cast<uint8_t>(conflicted_read_value[3]))); 
                                            
                                            dep_values.push_back(parsed_value);
                                        }
                                    }
                                }
    
                                // Re-compute output.
                                if(dep_values.size() > 0){
                                    new_value = dep_values.front() - 20;
                                    new_val_bytes[0] = static_cast<char>((new_value >> 24) & 0xFF);
                                    new_val_bytes[1] = static_cast<char>((new_value >> 16) & 0xFF);
                                    new_val_bytes[2] = static_cast<char>((new_value >> 8) & 0xFF);
                                    new_val_bytes[3] = static_cast<char>(new_value & 0xFF);
                                    value = Slice(new_val_bytes, 4);
                                    sa2 = GetWriteBatch()->GetWriteBatch()->Put(cfh, keyx, value);
                                    // if (!sa2.ok()) {
                                    //     return sa2;
                                    // }
                                }
                                break;
                            }
                        case 2: {
                            // DepositChecking
                            // Assume all deposit increments are in values of 10 for right now.

                            // For each dep key, get is value if it exists in the set of conflicted read keys.
                            for(const auto& dep : dep_keys){
                                // dep_key_value = dep.value;

                                // If this conflicted read key matches the dep key, get its value.
                                // Find conflicted read key that matches the dep key.

                                // Iterate over conflicted_read_keys.
                                for(const auto& pair : conflicted_read_keys){
                                    if(pair.first == dep.key){
                                        conflicted_read_value = pair.second;
                                        assert(conflicted_read_value.size() == 4);
                                        parsed_value = 
                                            ((static_cast<uint8_t>(conflicted_read_value[0]) << 24) |
                                            (static_cast<uint8_t>(conflicted_read_value[1]) << 16) |
                                            (static_cast<uint8_t>(conflicted_read_value[2]) << 8)  |
                                            (static_cast<uint8_t>(conflicted_read_value[3]))); 
                                        
                                        dep_values.push_back(parsed_value);
                                    }
                                }
                            }

                            // Re-compute output.
                            if(dep_values.size() > 0){
                                new_value = dep_values.front() + 10;
                                new_val_bytes[0] = static_cast<char>((new_value >> 24) & 0xFF);
                                new_val_bytes[1] = static_cast<char>((new_value >> 16) & 0xFF);
                                new_val_bytes[2] = static_cast<char>((new_value >> 8) & 0xFF);
                                new_val_bytes[3] = static_cast<char>(new_value & 0xFF);
                                value = Slice(new_val_bytes, 4);
                                sa2 = GetWriteBatch()->GetWriteBatch()->Put(cfh, keyx, value);
                                // if (!sa2.ok()) {
                                //     return sa2;
                                // }
                            }
                            break;
                        }
                        case 3: {
                            // TransactSaving
                            // All deltas are fixed in +30 for now.

                            // For each dep key, get is value if it exists in the set of conflicted read keys.
                            for(const auto& dep : dep_keys){
                                // dep_key_value = dep.value;

                                // If this conflicted read key matches the dep key, get its value.
                                // Find conflicted read key that matches the dep key.

                                // Iterate over conflicted_read_keys.
                                for(const auto& pair : conflicted_read_keys){
                                    if(pair.first == dep.key){
                                        conflicted_read_value = pair.second;
                                        assert(conflicted_read_value.size() == 4);

                                        parsed_value = 
                                            ((static_cast<uint8_t>(conflicted_read_value[0]) << 24) |
                                            (static_cast<uint8_t>(conflicted_read_value[1]) << 16) |
                                            (static_cast<uint8_t>(conflicted_read_value[2]) << 8)  |
                                            (static_cast<uint8_t>(conflicted_read_value[3]))); 
                                        
                                        dep_values.push_back(parsed_value);
                                    }
                                }
                            }

                            // Re-compute output.
                            if(dep_values.size() > 0){
                                new_value = dep_values.front() + 30;
                                new_val_bytes[0] = static_cast<char>((new_value >> 24) & 0xFF);
                                new_val_bytes[1] = static_cast<char>((new_value >> 16) & 0xFF);
                                new_val_bytes[2] = static_cast<char>((new_value >> 8) & 0xFF);
                                new_val_bytes[3] = static_cast<char>(new_value & 0xFF);
                                value = Slice(new_val_bytes, 4);
                                sa2 = GetWriteBatch()->GetWriteBatch()->Put(cfh, keyx, value);
                                // if (!sa2.ok()) {
                                //     return sa2;
                                // }
                            }
                            break;
                        }
                        case 4:
                            // Amalgamate
                            break;
                        default:
                            // Balance
                            break;
                    }

            
                    // if(conflicted_read_keys.find(key_bytes) != conflicted_read_keys.end()){
                    //         // std::cout << "Conflicted read key: " << key_bytes << std::endl;
                    // }


                    // Get the value read for this key, and use it to re-compute output of this write.
                    // Assume all deposit increments are in values of 10 for right now.


        
                    // TODO: Figure out how we can actually modify this write batch appropriately.

                    // NOW HERE WE SHOULD BE ABLE TO COMPUTE REPAIRED VALUE FOR KEY UPDATE!!!!
                    // if(parsed_value > 0){
                    //     // INSERT_YOUR_CODE
                    //     // Store parsed_value + 10 as the new value to be written into the batch, big-endian.
                    //     int32_t new_value = parsed_value + 10;
                    //     char new_val_bytes[4];
                    //     new_val_bytes[0] = static_cast<char>((new_value >> 24) & 0xFF);
                    //     new_val_bytes[1] = static_cast<char>((new_value >> 16) & 0xFF);
                    //     new_val_bytes[2] = static_cast<char>((new_value >> 8) & 0xFF);
                    //     new_val_bytes[3] = static_cast<char>(new_value & 0xFF);
                    //     value = Slice(new_val_bytes, 4);
            
                    //     Status sa = GetWriteBatch()->GetWriteBatch()->Put(cfh, keyx, value);
                    //     if (!sa.ok()) {
                    //     return sa;
                    //     }
                    // }
            }
        }
    }
//   /////////////////////////
//   Slice key1("key1");
//   Slice value("modified");
//   Status sa = GetWriteBatch()->GetWriteBatch()->Put(key1, value);
//   if (!sa.ok()) {
//     return sa;
//   }
//   ///////////////////////////////


  // Instead of aborting, we could modify the write batch here?



                   
  bool repair_mode = isolation_abort_mode == 3 && !conflicted_read_keys.empty();
  if (!s.ok() && !repair_mode) {
    return s;
  }


  // Return set of conflicted read keys.

  // For set of invalidated reads. Could re-compute output of writes based on new read values.



  s = db_impl->Write(write_options_, GetWriteBatch()->GetWriteBatch());
  if (s.ok()) {
    Clear();
  }

  return s;
}

Status OptimisticTransaction::Rollback() {
  Clear();
  return Status::OK();
}

// Record this key so that we can check it for conflicts at commit time.
//
// 'exclusive' is unused for OptimisticTransaction.
Status OptimisticTransaction::TryLock(ColumnFamilyHandle* column_family,
                                      const Slice& key, bool read_only,
                                      bool exclusive, const bool do_validate,
                                      const bool assume_tracked) {
  assert(!assume_tracked);  // not supported
  (void)assume_tracked;
  if (!do_validate) {
    return Status::OK();
  }
  uint32_t cfh_id = GetColumnFamilyID(column_family);

  SetSnapshotIfNeeded();

  SequenceNumber seq;
  if (snapshot_) {
    seq = snapshot_->GetSequenceNumber();
  } else {
    seq = db_->GetLatestSequenceNumber();
  }

  std::string key_str = key.ToString();

  TrackKey(cfh_id, key_str, seq, read_only, exclusive);

  // Always return OK. Confilct checking will happen at commit time.
  return Status::OK();
}

// Returns OK if it is safe to commit this transaction.  Returns Status::Busy
// if there are read or write conflicts that would prevent us from committing OR
// if we can not determine whether there would be any such conflicts.
//
// Should only be called on writer thread in order to avoid any race conditions
// in detecting write conflicts.
Status OptimisticTransaction::CheckTransactionForConflicts(DB* db) {
  auto db_impl = static_cast_with_check<DBImpl>(db);

  // Since we are on the write thread and do not want to block other writers,
  // we will do a cache-only conflict check.  This can result in TryAgain
  // getting returned if there is not sufficient memtable history to check
  // for conflicts.
  std::set<std::pair<std::string, std::string>> conflicted_read_keys;
  return TransactionUtil::CheckKeysForConflicts(db_impl, *tracked_locks_,
                                                true /* cache_only */,
                                                conflicted_read_keys);
}

Status OptimisticTransaction::SetName(const TransactionName& /* unused */) {
  return Status::InvalidArgument("Optimistic transactions cannot be named.");
}

}  // namespace ROCKSDB_NAMESPACE
