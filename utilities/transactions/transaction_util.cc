//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/transactions/transaction_util.h"

#include <cinttypes>
#include <string>
#include <vector>
#include <iostream>
#include "db/db_impl/db_impl.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "util/cast_util.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

Status TransactionUtil::CheckKeyForConflicts(
    DBImpl* db_impl, ColumnFamilyHandle* column_family, const std::string& key,
    SequenceNumber snap_seq, const std::string* const read_ts, bool cache_only,
    ReadCallback* snap_checker, SequenceNumber min_uncommitted,
    bool enable_udt_validation) {
  Status result;

  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  auto cfd = cfh->cfd();
  SuperVersion* sv = db_impl->GetAndRefSuperVersion(cfd);

  if (sv == nullptr) {
    result = Status::InvalidArgument("Could not access column family " +
                                     cfh->GetName());
  }

  if (result.ok()) {
    SequenceNumber earliest_seq =
        db_impl->GetEarliestMemTableSequenceNumber(sv, true);

    result =
        CheckKey(db_impl, sv, earliest_seq, snap_seq, key, read_ts, cache_only,
                 snap_checker, min_uncommitted, enable_udt_validation);

    db_impl->ReturnAndCleanupSuperVersion(cfd, sv);
  }

  return result;
}

Status TransactionUtil::CheckKey(DBImpl* db_impl, SuperVersion* sv,
                                 SequenceNumber earliest_seq,
                                 SequenceNumber snap_seq,
                                 const std::string& key,
                                 const std::string* const read_ts,
                                 bool cache_only, ReadCallback* snap_checker,
                                 SequenceNumber min_uncommitted,
                                 bool enable_udt_validation) {
  // When `min_uncommitted` is provided, keys are not always committed
  // in sequence number order, and `snap_checker` is used to check whether
  // specific sequence number is in the database is visible to the transaction.
  // So `snap_checker` must be provided.
  assert(min_uncommitted == kMaxSequenceNumber || snap_checker != nullptr);

  Status result;
  bool need_to_read_sst = false;

  //
  // WILL SCHULTZ: Checking keys for commit conflicts here!!!
  //

  // Since it would be too slow to check the SST files, we will only use
  // the memtables to check whether there have been any recent writes
  // to this key after it was accessed in this transaction.  But if the
  // Memtables do not contain a long enough history, we must fail the
  // transaction.
  if (earliest_seq == kMaxSequenceNumber) {
    // The age of this memtable is unknown.  Cannot rely on it to check
    // for recent writes.  This error shouldn't happen often in practice as
    // the Memtable should have a valid earliest sequence number except in some
    // corner cases (such as error cases during recovery).
    need_to_read_sst = true;

    if (cache_only) {
      result = Status::TryAgain(
          "Transaction could not check for conflicts as the MemTable does not "
          "contain a long enough history to check write at SequenceNumber: ",
          std::to_string(snap_seq));
    }
  } else if (snap_seq < earliest_seq || min_uncommitted <= earliest_seq) {
    // Use <= for min_uncommitted since earliest_seq is actually the largest sec
    // before this memtable was created
    need_to_read_sst = true;

    if (cache_only) {
      // The age of this memtable is too new to use to check for recent
      // writes.
      char msg[300];
      snprintf(msg, sizeof(msg),
               "Transaction could not check for conflicts for operation at "
               "SequenceNumber %" PRIu64
               " as the MemTable only contains changes newer than "
               "SequenceNumber %" PRIu64
               ".  Increasing the value of the "
               "max_write_buffer_size_to_maintain option could reduce the "
               "frequency "
               "of this error.",
               snap_seq, earliest_seq);
      result = Status::TryAgain(msg);
    }
  }

  if (result.ok()) {
    SequenceNumber seq = kMaxSequenceNumber;
    std::string timestamp;
    bool found_record_for_key = false;

    // When min_uncommitted == kMaxSequenceNumber, writes are committed in
    // sequence number order, so only keys larger than `snap_seq` can cause
    // conflict.
    // When min_uncommitted != kMaxSequenceNumber, keys lower than
    // min_uncommitted will not triggered conflicts, while keys larger than
    // min_uncommitted might create conflicts, so we need  to read them out
    // from the DB, and call callback to snap_checker to determine. So only
    // keys lower than min_uncommitted can be skipped.
    SequenceNumber lower_bound_seq =
        (min_uncommitted == kMaxSequenceNumber) ? snap_seq : min_uncommitted;
    
    // Look for latest update to this key.
    Status s = db_impl->GetLatestSequenceForKey(
        sv, key, !need_to_read_sst, lower_bound_seq, &seq,
        !read_ts ? nullptr : &timestamp, &found_record_for_key,
        /*is_blob_index=*/nullptr);

    if (!(s.ok() || s.IsNotFound() || s.IsMergeInProgress())) {
      result = s;
    } else if (found_record_for_key) {
      bool write_conflict = snap_checker == nullptr
                                ? snap_seq < seq
                                : !snap_checker->IsVisible(seq);
      // Pretty sure this will be same code for both write-write conflicts and read-write (via GetForUpdate) conflicts.
    //   std::cout << "    snap_seq: " << snap_seq << ", seq: " << seq << ", write_conflict: " << write_conflict << std::endl;
      // Perform conflict checking based on timestamp if applicable.

      // Did someone else write to a key that I also wrote to, AND did they also write to a key that I read from?
      // Should be able to do this below as long as we explicitly classify conflicts as write-write or read-write??
      if (enable_udt_validation && !write_conflict && read_ts != nullptr) {
        ColumnFamilyData* cfd = sv->cfd;
        assert(cfd);
        const Comparator* const ucmp = cfd->user_comparator();
        assert(ucmp);
        assert(read_ts->size() == ucmp->timestamp_size());
        assert(read_ts->size() == timestamp.size());
        // Write conflict if *ts < timestamp.
        write_conflict = ucmp->CompareTimestamp(*read_ts, timestamp) < 0;
      }
      if (write_conflict) {
        result = Status::Busy();
      }
    }
  }

  return result;
}

Status TransactionUtil::CheckKeysForConflicts(DBImpl* db_impl,
                                              const LockTracker& tracker,
                                              bool cache_only) {
  Status result;

  int isolation_abort_mode = 0;

  // INSERT_YOUR_CODE
  const char* abort_mode_env = std::getenv("ABORT_MODE");
  if (abort_mode_env != nullptr) {
    isolation_abort_mode = std::atoi(abort_mode_env);
  }

  std::unique_ptr<LockTracker::ColumnFamilyIterator> cf_it(
      tracker.GetColumnFamilyIterator());
  assert(cf_it != nullptr);
  while (cf_it->HasNext()) {
    ColumnFamilyId cf = cf_it->Next();

    SuperVersion* sv = db_impl->GetAndRefSuperVersion(cf);
    if (sv == nullptr) {
      result = Status::InvalidArgument("Could not access column family " +
                                       std::to_string(cf));
      break;
    }

    SequenceNumber earliest_seq =
        db_impl->GetEarliestMemTableSequenceNumber(sv, true);

    // For each of the keys in this transaction, check to see if someone has
    // written to this key since the start of the transaction.
    std::unique_ptr<LockTracker::KeyIterator> key_it(
        tracker.GetKeyIterator(cf));

    // WILL SCHULTZ: Checking all keys in txn here.
    // std::cout << "Checking all keys in txn: " << std::endl;
    assert(key_it != nullptr);

    //
    // If we wrote to a key, then it will be tracked as a written key. If we
    // read a key using GetForUpdate, then it will also be marked similarly, but
    // will have a special 'read_only' flag set on it, denoting that it was a
    // read, even though the conflict checking logic otherwise can be treated
    // essentially the same.
    //
    bool rw_conflict = false;
    bool ww_conflict = false;
    while (key_it->HasNext()) {
      const std::string& key = key_it->Next();
    //   std::cout << "  Checking key I wrote for conflict: " << key << std::endl;
      // Think I will need to know whether I read or wrote this key?
      PointLockStatus status = tracker.GetPointLockStatus(cf, key);
    //   std::cout << "  status.had_read: " << status.had_read << std::endl;
    //   std::cout << "  status.had_write: " << status.had_write << std::endl;
      const SequenceNumber key_seq = status.seq;

      // TODO: support timestamp-based conflict checking.
      // CheckKeysForConflicts() is currently used only by optimistic
      // transactions.
      result = CheckKey(db_impl, sv, earliest_seq, key_seq, key,
                        /*read_ts=*/nullptr, cache_only);

      // If there is a conflict, this indicates someone else concurrently wrote to 
      // this key. If this is a "read_only" key this indicates that I read it and
      // someone else concurrently wrote it, so we mark it as an outgoing rw-conflict.
      // Otherwise, it is marked as an incoming write-write conflict.
      if(!result.ok()){

         // Check if we wrote the key.
        if(status.had_write){
            ww_conflict = true;
        }

        // If we also marked the key as being read, then mark the conflict.
        // It is possible we both read and wrote it.
        if(status.had_read) {
          rw_conflict = true;
        //   std::cout << "  rw_conflict: " << rw_conflict << std::endl;
        } 
       
      }
    }


    result = Status::OK();
    if(isolation_abort_mode == 0){
        // No conflict checking.
        result = Status::OK();
    }
    else if(isolation_abort_mode == 1){
        if(ww_conflict){
            // Classic SI check.
            result = Status::Busy();
        }
    }
    else if(isolation_abort_mode == 2){
        if(rw_conflict && ww_conflict){
            // Refined SI check.
            result = Status::Busy();
        }
    }
    // Classic write snapshot isolation (only check RW conflicts).
    else if(isolation_abort_mode == 3){
        if(rw_conflict){
            result = Status::Busy();
        }
    }
   

    // std::cout << "  rw_conflict: " << rw_conflict << ", ww_conflict: " << ww_conflict << std::endl;

    db_impl->ReturnAndCleanupSuperVersion(cf, sv);

    // 
    // This below basically all we need to implement validation for refined SI algorithm?
    // Can probably do it eagerly above more appropriately.
    // 

    // if(rw_conflict && ww_conflict) {
    //   result = Status::Busy();
    // }

    if (!result.ok()) {
      break;
    }
  }

  return result;
}

}  // namespace ROCKSDB_NAMESPACE
