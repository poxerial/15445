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

#include <algorithm>
#include <memory>
#include <mutex>
#include <queue>
#include <stack>
#include <type_traits>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "concurrency/lock_manager.h"
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

static auto IsCompatible(const std::list<LockManager::LockRequest *> &request_queue, LockManager::LockMode lock_mode,
                         std::vector<txn_id_t> &incompatible_txns) -> bool {
  auto is_compatible = true;
  for (const auto request : request_queue) {
    if (!request->granted_) {
      return is_compatible;
    }
    if (!IS_COMPATIBLE(request->lock_mode_, lock_mode)) {
      is_compatible = false;
      incompatible_txns.push_back(request->txn_id_);
    }
  }
  return is_compatible;
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

auto AddEdges(std::unordered_map<txn_id_t, std::vector<txn_id_t>> &waits_for,
              const std::vector<txn_id_t> &incompatible_txns, txn_id_t txn) {
  auto iter = waits_for.find(txn);
  if (iter == waits_for.end()) {
    waits_for[txn] = {};
  }

  auto &v = waits_for[txn];
  for (const auto &incompatible_txn : incompatible_txns) {
    v.push_back(incompatible_txn);
  }
}

auto RemoveEdges(std::unordered_map<txn_id_t, std::vector<txn_id_t>> &waits_for,
                 const std::vector<txn_id_t> &incompatible_txns, txn_id_t txn) {
  for (const auto &incompatible_txn : incompatible_txns) {
    auto iter = waits_for.find(incompatible_txn);
    if (iter == waits_for.end()) {
      throw ExecutionException("Attempt to remove non-existent edge!");
    }

    auto v = iter->second;
    auto i = std::find(v.begin(), v.end(), txn);

    assert(i != v.end());

    v.erase(i);
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
    auto incompatible_txns = std::vector<txn_id_t>{};
    LockRequest *upgraded_request;
    for (auto request : lock_queue->request_queue_) {
      if (!request->granted_) {
        break;
      }
      if (request->txn_id_ == txn->GetTransactionId()) {
        upgraded_request = request;
        continue;
      }
      if (!detail::COMPATIBILITY_MATRIX[static_cast<int>(request->lock_mode_)][static_cast<int>(lock_mode)]) {
        compatible = false;
        incompatible_txns.push_back(request->txn_id_);
      }
    }

    if (!compatible) {
      lock_queue->upgrading_ = txn->GetTransactionId();
      lock_queue->upgrade_to_ = lock_mode;

      waits_for_latch_.lock();
      AddEdges(waits_for_, incompatible_txns, txn->GetTransactionId());
      waits_for_latch_.unlock();

      txn->UnlockTxn();

      for (;;) {
        lock_queue->cv_.wait(l);

        txn->LockTxn();
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_queue->upgrading_ = INVALID_TXN_ID;
          txn->UnlockTxn();
          return false;
        }
        txn->UnlockTxn();

        if (upgraded_request->lock_mode_ == lock_mode) {
          l.unlock();
          break;
        }
      }

      txn->LockTxn();
    } else {
      auto old_lock_mode = upgraded_request->lock_mode_;

      auto new_incompatible_txns = std::vector<txn_id_t>{};

      auto iter = lock_queue->request_queue_.begin();
      for (; iter != lock_queue->request_queue_.end() && (*iter)->granted_; ++iter) {
      }

      for (; iter != lock_queue->request_queue_.end(); ++iter) {
        if (IS_COMPATIBLE(old_lock_mode, (*iter)->lock_mode_) && !IS_COMPATIBLE(lock_mode, (*iter)->lock_mode_)) {
          new_incompatible_txns.push_back((*iter)->txn_id_);
        }
      }

      waits_for_latch_.lock();
      for (const auto &new_incompatible_txn : new_incompatible_txns) {
        auto i = waits_for_.find(new_incompatible_txn);
        if (i == waits_for_.end()) {
          waits_for_[new_incompatible_txn] = {};
        }
        waits_for_[new_incompatible_txn].push_back(txn->GetTransactionId());
      }
      waits_for_latch_.unlock();

      upgraded_request->lock_mode_ = lock_mode;
      l.unlock();
    }

    upgrade_from_lock_set->erase(oid);

    lock_set->insert(oid);

    txn->UnlockTxn();
    return true;
  }
  auto request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  auto incompatible_txns = std::vector<txn_id_t>{};
  if (lock_queue->request_queue_.empty() || IsCompatible(lock_queue->request_queue_, lock_mode, incompatible_txns)) {
    request->granted_ = true;
    lock_queue->request_queue_.push_back(request);

    l.unlock();
  } else {
    lock_queue->request_queue_.push_back(request);

    waits_for_latch_.lock();
    AddEdges(waits_for_, incompatible_txns, txn->GetTransactionId());
    waits_for_latch_.unlock();

    txn->UnlockTxn();

    for (;;) {
      lock_queue->cv_.wait(l);

      txn->LockTxn();
      if (txn->GetState() == TransactionState::ABORTED) {

        incompatible_txns = {};

        IsCompatible(lock_queue->request_queue_, lock_mode, incompatible_txns);
        waits_for_latch_.lock();
        auto &v = waits_for_[txn->GetTransactionId()];
        for (const auto &incompatible_txn : incompatible_txns) {
          v.erase(std::find(v.begin(), v.end(), incompatible_txn));
        }
        waits_for_latch_.unlock();

        auto this_request_iter = lock_queue->request_queue_.begin();
        for (;; ++this_request_iter) {
          assert(this_request_iter != lock_queue->request_queue_.end());
          if ((*this_request_iter)->txn_id_ == txn->GetTransactionId()) {
            lock_queue->request_queue_.erase(this_request_iter);
          }
        }

        delete request;

        txn->UnlockTxn();
        return false;
      }
      txn->UnlockTxn();

      if (request->granted_) {
        l.unlock();
        break;
      }
    }

    txn->LockTxn();
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
      auto mode = (*i)->lock_mode_;
      auto granted = (*i)->granted_;

      delete (*i);
      i = lock_queue->request_queue_.erase(i);

      if (granted) {
        auto incompatible_txns = std::vector<txn_id_t>{};

        for (; i != lock_queue->request_queue_.end(); ++i) {
          if (!IS_COMPATIBLE(mode, (*i)->lock_mode_)) {
            incompatible_txns.push_back((*i)->txn_id_);
          }
        }

        if (incompatible_txns.empty() && lock_queue->upgrading_ == INVALID_TXN_ID) {
          break;
        }

        waits_for_latch_.lock();
        RemoveEdges(waits_for_, incompatible_txns, txn->GetTransactionId());
        if (lock_queue->upgrading_ != INVALID_TXN_ID) {
          auto iter = waits_for_.find(lock_queue->upgrading_);
          assert(iter != waits_for_.end());
          auto v = iter->second;
          auto v_iter = std::find(v.begin(), v.end(), txn->GetTransactionId());
          if (v_iter != v.end()) {
            v.erase(v_iter);
          }
        }
        waits_for_latch_.unlock();

        break;
      }
      auto incompatible_txns = std::vector<txn_id_t>{};
      IsCompatible(lock_queue->request_queue_, (*i)->lock_mode_, incompatible_txns);

      waits_for_latch_.lock();
      auto &v = waits_for_[(*i)->txn_id_];
      for (auto txn : incompatible_txns) {
        v.erase(std::find(v.begin(), v.end(), txn));
      }
      waits_for_latch_.unlock();
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
    for (auto request : lock_queue->request_queue_) {
      if (!request->granted_) {
        break;
      }
      if (request->txn_id_ == lock_queue->upgrading_) {
        upgraded_request = request;
        continue;
      }
      if (!IS_COMPATIBLE(request->lock_mode_, lock_queue->upgrade_to_)) {
        compatible = false;
      }
    }

    if (compatible) {
      auto old_lock_mode = upgraded_request->lock_mode_;

      auto new_incompatible_txns = std::vector<txn_id_t>{};

      auto iter = lock_queue->request_queue_.begin();
      for (; iter != lock_queue->request_queue_.end() && (*iter)->granted_; ++iter) {
      }

      for (; iter != lock_queue->request_queue_.end(); ++iter) {
        if (IS_COMPATIBLE(old_lock_mode, (*iter)->lock_mode_) &&
            !IS_COMPATIBLE(lock_queue->upgrade_to_, (*iter)->lock_mode_)) {
          new_incompatible_txns.push_back((*iter)->txn_id_);
        }
      }

      waits_for_latch_.lock();
      for (const auto &new_incompatible_txn : new_incompatible_txns) {
        auto i = waits_for_.find(new_incompatible_txn);
        if (i == waits_for_.end()) {
          waits_for_[new_incompatible_txn] = {};
        }
        waits_for_[new_incompatible_txn].push_back(txn->GetTransactionId());
      }
      waits_for_latch_.unlock();

      upgraded_request->lock_mode_ = lock_queue->upgrade_to_;
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

      waits_for_latch_.lock();
      for (const auto &request : lock_queue->request_queue_) {
        if (!request->granted_ && !IS_COMPATIBLE((*curr)->lock_mode_, request->lock_mode_)) {
          waits_for_[request->txn_id_].push_back((*curr)->txn_id_);
        }
      }
      waits_for_latch_.unlock();
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
    auto incompatible_txns = std::vector<txn_id_t>{};
    LockRequest *upgraded_request;
    for (auto request : lock_queue->request_queue_) {
      if (!request->granted_) {
        break;
      }
      if (request->txn_id_ == txn->GetTransactionId()) {
        upgraded_request = request;
        continue;
      }
      if (!IS_COMPATIBLE(request->lock_mode_, lock_mode)) {
        incompatible_txns.push_back(request->txn_id_);
        compatible = false;
      }
    }

    if (!compatible) {
      lock_queue->upgrading_ = txn->GetTransactionId();
      lock_queue->upgrade_to_ = lock_mode;

      waits_for_latch_.lock();
      AddEdges(waits_for_, incompatible_txns, txn->GetTransactionId());
      waits_for_latch_.unlock();

      txn->UnlockTxn();

      for (;;) {
        lock_queue->cv_.wait(l);

        txn->LockTxn();
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_queue->upgrading_ = INVALID_TXN_ID;
          txn->UnlockTxn();
          return false;
        }
        txn->UnlockTxn();

        if (upgraded_request->lock_mode_ == lock_mode) {
          l.unlock();
          break;
        }
      }

      txn->LockTxn();
    } else {
      upgraded_request->lock_mode_ = lock_mode;

      auto old_lock_mode = upgraded_request->lock_mode_;

      auto new_incompatible_txns = std::vector<txn_id_t>{};

      auto iter = lock_queue->request_queue_.begin();
      for (; iter != lock_queue->request_queue_.end() && (*iter)->granted_; ++iter) {
      }

      for (; iter != lock_queue->request_queue_.end(); ++iter) {
        if (IS_COMPATIBLE(old_lock_mode, (*iter)->lock_mode_) && !IS_COMPATIBLE(lock_mode, (*iter)->lock_mode_)) {
          new_incompatible_txns.push_back((*iter)->txn_id_);
        }
      }

      waits_for_latch_.lock();
      for (const auto &new_incompatible_txn : new_incompatible_txns) {
        auto i = waits_for_.find(new_incompatible_txn);
        if (i == waits_for_.end()) {
          waits_for_[new_incompatible_txn] = {};
        }
        waits_for_[new_incompatible_txn].push_back(txn->GetTransactionId());
      }
      waits_for_latch_.unlock();

      l.unlock();
    }

    auto upgrade_from_lock_set = txn->GetSharedRowLockSet();
    upgrade_from_lock_set->erase(oid);

    (*lock_set)[oid].insert(rid);

    txn->UnlockTxn();
    return true;
  }

  auto request = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);
  auto incompatible_txns = std::vector<txn_id_t>{};
  if (lock_queue->request_queue_.empty() || IsCompatible(lock_queue->request_queue_, lock_mode, incompatible_txns)) {
    request->granted_ = true;
    lock_queue->request_queue_.push_back(request);

    l.unlock();
  } else {
    lock_queue->request_queue_.push_back(request);

    waits_for_latch_.lock();
    AddEdges(waits_for_, incompatible_txns, txn->GetTransactionId());
    waits_for_latch_.unlock();

    txn->UnlockTxn();

    for (;;) {
      lock_queue->cv_.wait(l);

      txn->LockTxn();
      if (txn->GetState() == TransactionState::ABORTED) {

        incompatible_txns = {};

        IsCompatible(lock_queue->request_queue_, lock_mode, incompatible_txns);
        waits_for_latch_.lock();
        auto &v = waits_for_[txn->GetTransactionId()];
        for (const auto &incompatible_txn : incompatible_txns) {
          v.erase(std::find(v.begin(), v.end(), incompatible_txn));
        }
        waits_for_latch_.unlock();

        auto this_request_iter = lock_queue->request_queue_.begin();
        for (;; ++this_request_iter) {
          assert(this_request_iter != lock_queue->request_queue_.end());
          if ((*this_request_iter)->txn_id_ == txn->GetTransactionId()) {
            lock_queue->request_queue_.erase(this_request_iter);
            break;
          }
        }

        delete request;

        txn->UnlockTxn();
        return false;
      }
      txn->UnlockTxn();

      if (request->granted_) {
        l.unlock();
        break;
      }
    }

    txn->LockTxn();
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
      auto mode = (*i)->lock_mode_;
      auto granted = (*i)->granted_;

      delete (*i);
      i = lock_queue->request_queue_.erase(i);

      if (granted) {
        auto incompatible_txns = std::vector<txn_id_t>{};

        for (; i != lock_queue->request_queue_.end(); ++i) {
          if (!detail::COMPATIBILITY_MATRIX[static_cast<int>(mode)][static_cast<int>((*i)->lock_mode_)]) {
            incompatible_txns.push_back((*i)->txn_id_);
          }
        }

        if (incompatible_txns.empty() && lock_queue->upgrading_ == INVALID_TXN_ID) {
          break;
        }

        waits_for_latch_.lock();
        RemoveEdges(waits_for_, incompatible_txns, txn->GetTransactionId());
        if (lock_queue->upgrading_ != INVALID_TXN_ID) {
          auto iter = waits_for_.find(lock_queue->upgrading_);
          assert(iter != waits_for_.end());
          auto v = iter->second;
          auto v_iter = std::find(v.begin(), v.end(), txn->GetTransactionId());
          if (v_iter != v.end()) {
            v.erase(v_iter);
          }
        }
        waits_for_latch_.unlock();

        break;
      }
      auto incompatible_txns = std::vector<txn_id_t>{};
      IsCompatible(lock_queue->request_queue_, (*i)->lock_mode_, incompatible_txns);

      waits_for_latch_.lock();
      auto &v = waits_for_[(*i)->txn_id_];
      for (auto txn : incompatible_txns) {
        v.erase(std::find(v.begin(), v.end(), txn));
      }
      waits_for_latch_.unlock();
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
    for (auto request : lock_queue->request_queue_) {
      if (!request->granted_) {
        break;
      }
      if (request->txn_id_ == lock_queue->upgrading_) {
        upgraded_request = request;
        continue;
      }
      if (!IS_COMPATIBLE(request->lock_mode_, lock_queue->upgrade_to_)) {
        compatible = false;
      }
    }

    if (compatible) {
      auto old_lock_mode = upgraded_request->lock_mode_;

      auto new_incompatible_txns = std::vector<txn_id_t>{};

      auto iter = lock_queue->request_queue_.begin();
      for (; iter != lock_queue->request_queue_.end() && (*iter)->granted_; ++iter) {
      }

      for (; iter != lock_queue->request_queue_.end(); ++iter) {
        if (IS_COMPATIBLE(old_lock_mode, (*iter)->lock_mode_) &&
            !IS_COMPATIBLE(lock_queue->upgrade_to_, (*iter)->lock_mode_)) {
          new_incompatible_txns.push_back((*iter)->txn_id_);
        }
      }

      waits_for_latch_.lock();
      for (const auto &new_incompatible_txn : new_incompatible_txns) {
        auto i = waits_for_.find(new_incompatible_txn);
        if (i == waits_for_.end()) {
          waits_for_[new_incompatible_txn] = {};
        }
        waits_for_[new_incompatible_txn].push_back(txn->GetTransactionId());
      }
      waits_for_latch_.unlock();

      upgraded_request->lock_mode_ = lock_queue->upgrade_to_;

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

      waits_for_latch_.lock();
      for (const auto &request : lock_queue->request_queue_) {
        if (!request->granted_ && !IS_COMPATIBLE((*curr)->lock_mode_, request->lock_mode_)) {
          waits_for_[request->txn_id_].push_back((*curr)->txn_id_);
        }
      }
      waits_for_latch_.unlock();
    }
  }

  l.unlock();
  lock_queue->cv_.notify_all();

  (*lock_set)[oid].erase(rid);

  txn->UnlockTxn();
  UpdateTransactionState(txn, lock_mode);

  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  auto l = std::unique_lock<std::mutex>(waits_for_latch_);

  auto iter = waits_for_.find(t1);
  if (iter == waits_for_.end()) {
    waits_for_[t1] = std::vector<txn_id_t>{};
  }
  waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto l = std::unique_lock<std::mutex>(waits_for_latch_);

  auto iter = waits_for_.find(t1);
  if (iter == waits_for_.end()) {
    throw ExecutionException("Attempt to remove non-existent edge!");
  }
  auto &v = waits_for_[t1];
  v.erase(std::find(v.begin(), v.end(), t2));
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  auto l = std::unique_lock<std::mutex>(waits_for_latch_);

  auto no_cycle_waits_for = std::unordered_map<txn_id_t, std::vector<txn_id_t>>{};

  while (!waits_for_.empty()) {
    auto root = waits_for_.begin()->first;

    auto st = std::stack<std::pair<txn_id_t, std::size_t>>{};
    st.push({root, 0});

    auto fathers_set = std::unordered_set<txn_id_t>{};
    fathers_set.insert(root);

    while (!st.empty()) {
      auto txn = st.top().first;
      auto &v = waits_for_[st.top().first];

      if (v.size() == st.top().second) {
        no_cycle_waits_for[txn] = std::move(v);
        waits_for_.erase(txn);
        st.pop();
        fathers_set.erase(txn);
        continue;
      }

      auto next_txn = v[st.top().second++];

      if (fathers_set.count(next_txn) != 0) {
        // cycle!
        txn_id_t newest_txn = next_txn;
        for (const auto &father : fathers_set) {
          if (father > newest_txn) {
            newest_txn = father;
          }
        }

        *txn_id = newest_txn;
        return true;
      }

      fathers_set.insert(next_txn);
      st.push({next_txn, 0});
    }
  }

  waits_for_ = std::move(no_cycle_waits_for);
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);

  auto l = std::unique_lock<std::mutex>(waits_for_latch_);

  for (const auto &[a, v] : waits_for_) {
    for (const auto &b : v) {
      edges.emplace_back(a, b);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      txn_id_t txn;
      if (HasCycle(&txn)) {
        auto txn_p = TransactionManager::GetTransaction(txn);

        txn_p->LockTxn();

        txn_p = TransactionManager::GetTransaction(txn);
        if (txn_p == nullptr) {
          continue;
        }

        txn_p->SetState(TransactionState::ABORTED);
        txn_p->UnlockTxn();

        row_lock_map_latch_.lock();
        for (const auto &[rid, queue] : row_lock_map_) {
          queue->cv_.notify_all();
        }
        row_lock_map_latch_.unlock();
      }
    }
  }
}

}  // namespace bustub
