//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <algorithm>
#include <memory>
#include <mutex>
#include <type_traits>

#include "common/config.h"
#include "common/exception.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

namespace detail {
constexpr const static bool COMPATIBILITY_MATRIX[5][5] = {
    /*==========MGL COMPARIBILITY MATRIX========*/
    /*            S     X      IS    IX    SIX  */
    /* S    */ {true, false, true, false, false},
    /* X    */ {false, false, false, false, false},
    /* IS   */ {true, false, true, true, true},
    /* IX   */ {false, false, true, true, false},
    /* SIX  */ {false, false, true, false, false}
    /*==========================================*/
};

constexpr const static bool UPGRADE_COMPATIBILITY_MATRIX[5][5] = {
    /*========UPGRADE COMPARIBILITY MATRIX======*/
    /*            S     X      IS    IX    SIX  */
    /* S    */ {false, true, false, false, true},
    /* X    */ {false, false, false, false, false},
    /* IS   */ {true, true, false, true, true},
    /* IX   */ {false, true, false, false, true},
    /* SIX  */ {false, true, false, false, false}
    /*==========================================*/
};
};  // namespace detail

#define IS_COMPATIBLE(A, B) (detail::COMPATIBILITY_MATRIX[static_cast<int>(A)][static_cast<int>(B)])

static auto IsCompatible(const std::list<LockManager::LockRequest *> &request_queue, LockManager::LockMode lock_mode)
    -> bool {
  for (const auto request : request_queue) {
    if (!request->granted_) {
      return true;
    }
    if (!IS_COMPATIBLE(request->lock_mode_, lock_mode)) {
      return false;
    }
  }
  return true;
}

static auto GetTableLockSet(Transaction *txn, LockManager::LockMode lock_mode)
    -> std::shared_ptr<std::unordered_set<table_oid_t>> {
  switch (lock_mode) {
    case LockManager::LockMode::SHARED:
      return txn->GetSharedTableLockSet();
    case LockManager::LockMode::EXCLUSIVE:
      return txn->GetExclusiveTableLockSet();
    case LockManager::LockMode::INTENTION_SHARED:
      return txn->GetIntentionSharedTableLockSet();
    case LockManager::LockMode::INTENTION_EXCLUSIVE:
      return txn->GetIntentionExclusiveTableLockSet();
    case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
      return txn->GetSharedIntentionExclusiveTableLockSet();
  }
}

static auto GetRowLockSet(Transaction *txn, LockManager::LockMode lock_mode)
    -> std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> {
  switch (lock_mode) {
    case LockManager::LockMode::SHARED:
      return txn->GetSharedRowLockSet();
    case LockManager::LockMode::EXCLUSIVE:
      return txn->GetExclusiveRowLockSet();
    case LockManager::LockMode::INTENTION_SHARED:
    case LockManager::LockMode::INTENTION_EXCLUSIVE:
    case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
      throw ExecutionException(__func__);
  }
}

static auto BasicCheck(Transaction *txn, LockManager::LockMode lock_mode) -> void {
  // basic check
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
      if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockManager::LockMode::INTENTION_SHARED &&
          lock_mode != LockManager::LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      if (lock_mode != LockManager::LockMode::EXCLUSIVE && lock_mode != LockManager::LockMode::INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      break;
  }
}

static auto CheckLockTable(Transaction *txn, LockManager::LockMode lock_mode, const table_oid_t &oid) -> void {
  BasicCheck(txn, lock_mode);
}

static auto CheckUpgrade(Transaction *txn, LockManager::LockMode upgrade_from, LockManager::LockMode lock_mode)
    -> void {
  if (!detail::UPGRADE_COMPATIBILITY_MATRIX[static_cast<int>(upgrade_from)][static_cast<int>(lock_mode)]) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
  }
}

static auto CheckLockRow(Transaction *txn, LockManager::LockMode lock_mode, const table_oid_t &oid, const RID &rid)
    -> void {
  BasicCheck(txn, lock_mode);

  // row lock must not be intention lock
  if (lock_mode == LockManager::LockMode::INTENTION_EXCLUSIVE || lock_mode == LockManager::LockMode::INTENTION_SHARED ||
      lock_mode == LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  // check table lock
  LockManager::LockMode table_lock_mode;
  std::shared_ptr<std::unordered_set<table_oid_t>> lock_set;
  for (auto i = 0; i < 5; i++) {
    table_lock_mode = static_cast<LockManager::LockMode>(i);
    lock_set = GetTableLockSet(txn, lock_mode);
    if (lock_set->count(oid) != 0) {
      break;
    }
  }

  auto table_lock_correct = true;

  if (lock_set == nullptr) {
    table_lock_correct = false;
  } else {
    switch (lock_mode) {
      case LockManager::LockMode::SHARED:
        if (table_lock_mode == LockManager::LockMode::EXCLUSIVE ||
            table_lock_mode == LockManager::LockMode::INTENTION_EXCLUSIVE) {
          table_lock_correct = false;
        }
        break;
      case LockManager::LockMode::EXCLUSIVE:
        if (table_lock_mode == LockManager::LockMode::INTENTION_SHARED ||
            table_lock_mode == LockManager::LockMode::SHARED) {
          table_lock_correct = false;
        }
        break;
      case LockManager::LockMode::INTENTION_SHARED:
      case LockManager::LockMode::INTENTION_EXCLUSIVE:
      case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
        break;
    }
  }

  if (!table_lock_correct) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  txn->LockTxn();

  if (txn->GetState() == TransactionState::ABORTED) {
    txn->UnlockTxn();
    return false;
  }

  CheckLockTable(txn, lock_mode, oid);

  // check if is lock upgrading
  LockMode upgrade_from;
  bool is_upgrade = false;
  std::shared_ptr<std::unordered_set<table_oid_t>> upgrade_from_lock_set;
  for (auto i = 0; i < 5; i++) {
    upgrade_from = static_cast<LockMode>(i);
    upgrade_from_lock_set = GetTableLockSet(txn, upgrade_from);
    auto iter = upgrade_from_lock_set->find(oid);
    if (iter != upgrade_from_lock_set->end()) {
      if (lock_mode == upgrade_from) {
        txn->UnlockTxn();
        return true;
      }
      CheckUpgrade(txn, upgrade_from, lock_mode);
      is_upgrade = true;
      break;
    }
  }

  table_lock_map_latch_.lock();

  if (table_lock_map_.count(oid) == 0) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto lock_queue = table_lock_map_[oid];

  table_lock_map_latch_.unlock();

  auto l = std::unique_lock<std::mutex>(lock_queue->latch_);

  auto lock_set = GetTableLockSet(txn, lock_mode);

  if (is_upgrade) {
    if (lock_queue->upgrading_ != INVALID_TXN_ID) {
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }

    auto compatible = true;
    LockRequest *upgraded_request;
    for (auto reqeust : lock_queue->request_queue_) {
      if (!reqeust->granted_) {
        break;
      }
      if (reqeust->txn_id_ == txn->GetTransactionId()) {
        upgraded_request = reqeust;
        continue;
      }
      if (!detail::COMPATIBILITY_MATRIX[static_cast<int>(reqeust->lock_mode_)][static_cast<int>(lock_mode)]) {
        compatible = false;
      }
    }

    if (!compatible) {
      lock_queue->upgrading_ = txn->GetTransactionId();

      for (;;) {
        lock_queue->cv_.wait(l);

        if (upgraded_request->lock_mode_ == lock_mode) {
          l.unlock();
          break;
        }
      }
    } else {
      upgraded_request->lock_mode_ = lock_mode;
      l.unlock();
    }

    upgrade_from_lock_set->erase(oid);

    lock_set->insert(oid);

    txn->UnlockTxn();
    return true;
  }
  auto request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  if (lock_queue->request_queue_.empty() || IsCompatible(lock_queue->request_queue_, lock_mode)) {
    request->granted_ = true;
    lock_queue->request_queue_.push_back(request);

    l.unlock();
  } else {
    lock_queue->request_queue_.push_back(request);

    for (;;) {
      lock_queue->cv_.wait(l);
      if (request->granted_) {
        l.unlock();
        break;
      }
    }
  }

  lock_set->insert(oid);

  txn->UnlockTxn();

  return true;
}

static auto UpdateTransactionState(Transaction *txn, LockManager::LockMode lock_mode) {
  if (txn->GetState() != TransactionState::GROWING) {
    return;
  }
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
      if (lock_mode == LockManager::LockMode::SHARED || lock_mode == LockManager::LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
    case IsolationLevel::READ_UNCOMMITTED:
      if (lock_mode == LockManager::LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
  }
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  txn->LockTxn();

  LockMode lock_mode;
  std::shared_ptr<std::unordered_set<table_oid_t>> lock_set;
  for (auto i = 0; i < 5; i++) {
    lock_mode = static_cast<LockMode>(i);
    lock_set = GetTableLockSet(txn, lock_mode);
    if (lock_set->count(oid) != 0) {
      break;
    }
  }

  if (lock_set == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  for (auto i = 0; i < 2; i++) {
    auto mode = static_cast<LockMode>(i);
    auto lock_set = GetRowLockSet(txn, mode);
    if (lock_set->count(oid) != 0 && !(*lock_set)[oid].empty()) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
    }
  }

  table_lock_map_latch_.lock();

  assert(table_lock_map_.count(oid) != 0);

  auto lock_queue = table_lock_map_[oid];

  table_lock_map_latch_.unlock();

  auto l = std::unique_lock<std::mutex>(lock_queue->latch_);

  for (auto i = lock_queue->request_queue_.begin(); i != lock_queue->request_queue_.end(); ++i) {
    if ((*i)->oid_ == oid) {
      delete (*i);
      lock_queue->request_queue_.erase(i);
      break;
    }
  }

  if (lock_queue->upgrading_ != INVALID_TXN_ID) {
    LockRequest *upgraded_request;
    for (auto request : lock_queue->request_queue_) {
      if (request->txn_id_ == lock_queue->upgrading_) {
        upgraded_request = request;
        break;
      }
    }

    auto compatible = true;
    for (auto reqeust : lock_queue->request_queue_) {
      if (!reqeust->granted_) {
        break;
      }
      if (reqeust->txn_id_ == lock_queue->upgrading_) {
        upgraded_request = reqeust;
        continue;
      }
      if (!detail::COMPATIBILITY_MATRIX[static_cast<int>(reqeust->lock_mode_)][static_cast<int>(lock_mode)]) {
        compatible = false;
      }
    }

    if (compatible) {
      upgraded_request->granted_ = true;
      lock_queue->upgrading_ = INVALID_TXN_ID;
    }
  }

  if (lock_queue->upgrading_ == INVALID_TXN_ID) {
    auto curr = lock_queue->request_queue_.begin();
    while (curr != lock_queue->request_queue_.end() && (*curr)->granted_) {
      ++curr;
    }
    for (; curr != lock_queue->request_queue_.end(); ++curr) {
      if (!IsCompatible(lock_queue->request_queue_, (*curr)->lock_mode_)) {
        break;
      }
      (*curr)->granted_ = true;
    }
  }

  l.unlock();
  lock_queue->cv_.notify_all();

  lock_set->erase(oid);

  UpdateTransactionState(txn, lock_mode);

  txn->UnlockTxn();

  return true;
}

static auto ContainsRow(const std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> &lock_set,
                        const table_oid_t &oid, const RID &rid) -> bool {
  auto table_iter = lock_set->find(oid);
  if (table_iter != lock_set->end()) {
    auto row_set = table_iter->second;
    auto row_iter = row_set.find(rid);
    if (row_iter != row_set.end()) {
      return true;
    }
  }
  return false;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  txn->LockTxn();

  if (txn->GetState() == TransactionState::ABORTED) {
    txn->UnlockTxn();
    return false;
  }

  CheckLockRow(txn, lock_mode, oid, rid);

  // check if is lock upgrading
  auto is_upgrade = false;
  for (auto i = 0; i < 2; i++) {
    auto mode = static_cast<LockMode>(i);
    auto lock_set = GetRowLockSet(txn, mode);
    if (ContainsRow(lock_set, oid, rid)) {
      // relock
      if (mode == lock_mode) {
        txn->UnlockTxn();
        return true;
      }
      CheckUpgrade(txn, mode, lock_mode);
      is_upgrade = true;
    }
  }

  row_lock_map_latch_.lock();

  if (row_lock_map_.count(rid) == 0) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  auto lock_queue = row_lock_map_[rid];

  row_lock_map_latch_.unlock();

  auto l = std::unique_lock<std::mutex>(lock_queue->latch_);

  auto lock_set = GetRowLockSet(txn, lock_mode);

  if (is_upgrade) {
    if (lock_queue->upgrading_ != INVALID_TXN_ID) {
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }

    auto compatible = true;
    LockRequest *upgraded_request;
    for (auto reqeust : lock_queue->request_queue_) {
      if (!reqeust->granted_) {
        break;
      }
      if (reqeust->txn_id_ == txn->GetTransactionId()) {
        upgraded_request = reqeust;
        continue;
      }
      if (!detail::COMPATIBILITY_MATRIX[static_cast<int>(reqeust->lock_mode_)][static_cast<int>(lock_mode)]) {
        compatible = false;
      }
    }

    if (!compatible) {
      lock_queue->upgrading_ = txn->GetTransactionId();

      for (;;) {
        lock_queue->cv_.wait(l);

        if (upgraded_request->lock_mode_ == lock_mode) {
          l.unlock();
          break;
        }
      }
    } else {
      upgraded_request->lock_mode_ = lock_mode;
      l.unlock();
    }

    auto upgrade_from_lock_set = txn->GetSharedRowLockSet();
    upgrade_from_lock_set->erase(oid);

    (*lock_set)[oid].insert(rid);

    txn->UnlockTxn();
    return true;
  }

  auto request = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);
  if (lock_queue->request_queue_.empty() || IsCompatible(lock_queue->request_queue_, lock_mode)) {
    request->granted_ = true;
    lock_queue->request_queue_.push_back(request);

    l.unlock();
  } else {
    lock_queue->request_queue_.push_back(request);

    for (;;) {
      lock_queue->cv_.wait(l);
      if (request->granted_) {
        l.unlock();
        break;
      }
    }
  }

  (*lock_set)[oid].insert(rid);

  txn->UnlockTxn();

  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  txn->LockTxn();

  LockMode lock_mode;
  std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> lock_set;
  for (auto i = 0; i < 2; i++) {
    lock_mode = static_cast<LockMode>(i);
    lock_set = GetRowLockSet(txn, lock_mode);
    if (ContainsRow(lock_set, oid, rid)) {
      break;
    }
  }

  if (lock_set == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  row_lock_map_latch_.lock();

  if (row_lock_map_.count(rid) == 0) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  auto lock_queue = row_lock_map_[rid];

  row_lock_map_latch_.unlock();

  auto l = std::unique_lock<std::mutex>(lock_queue->latch_);

  for (auto i = lock_queue->request_queue_.begin(); i != lock_queue->request_queue_.end(); ++i) {
    if ((*i)->txn_id_ == txn->GetTransactionId()) {
      delete (*i);
      lock_queue->request_queue_.erase(i);
      break;
    }
  }

  if (lock_queue->upgrading_ != INVALID_TXN_ID) {
    LockRequest *upgraded_request;
    for (auto request : lock_queue->request_queue_) {
      if (request->txn_id_ == lock_queue->upgrading_) {
        upgraded_request = request;
        break;
      }
    }

    auto compatible = true;
    for (auto reqeust : lock_queue->request_queue_) {
      if (!reqeust->granted_) {
        break;
      }
      if (reqeust->txn_id_ == lock_queue->upgrading_) {
        upgraded_request = reqeust;
        continue;
      }
      if (!detail::COMPATIBILITY_MATRIX[static_cast<int>(reqeust->lock_mode_)][static_cast<int>(lock_mode)]) {
        compatible = false;
      }
    }

    if (compatible) {
      upgraded_request->granted_ = true;
      lock_queue->upgrading_ = INVALID_TXN_ID;
    }
  }

  if (lock_queue->upgrading_ == INVALID_TXN_ID) {
    auto curr = lock_queue->request_queue_.begin();
    while (curr != lock_queue->request_queue_.end() && (*curr)->granted_) {
      ++curr;
    }
    for (; curr != lock_queue->request_queue_.end(); ++curr) {
      if (!IsCompatible(lock_queue->request_queue_, (*curr)->lock_mode_)) {
        break;
      }
      (*curr)->granted_ = true;
    }
  }

  l.unlock();
  lock_queue->cv_.notify_all();

  (*lock_set)[oid].erase(rid);

  txn->UnlockTxn();
  UpdateTransactionState(txn, lock_mode);

  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
