//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iterator>
#include <memory>
#include <unordered_map>
#include <utility>

#include "catalog/schema.h"
#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/type.h"

namespace bustub {

struct HashJoinKey {
  std::vector<Value> key_{};
  HashJoinKey(const std::vector<AbstractExpressionRef> &exprs, Tuple &t, const Schema &schema) {
    key_.reserve(exprs.size());
    for (const auto &expr : exprs) {
      key_.push_back(expr->Evaluate(&t, schema));
    }
  }
  auto operator==(const HashJoinKey& that) const -> bool {
    if (key_.size() != that.key_.size()) {
      return false;
    }
    for (std::size_t i = 0; i < key_.size(); i++) {
      if (key_[i].CompareEquals(that.key_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

struct HashJoinValue {
  explicit HashJoinValue(Tuple &t) : tuple_(t) {}
  Tuple tuple_;
  bool is_emitted_{false};
};

struct HashJoinValues {
  auto Append(Tuple &t) { tuples_.emplace_back(t); }
  std::vector<HashJoinValue> tuples_{};
};

}  // namespace bustub

namespace std {

template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &hash_join_key) const -> size_t {
    size_t curr_hash = 0;
    for (const auto &key : hash_join_key.key_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  using MapType = std::unordered_map<HashJoinKey, HashJoinValues>;
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  /** Left child */
  std::unique_ptr<AbstractExecutor> left_child_;
  /** Right child */
  std::unique_ptr<AbstractExecutor> right_child_;
  /** Hash map */
  MapType hash_map_{};

  // execution context
  /** Current right tuple */
  Tuple curr_right_;
  /** Whether current right tuple is valid */
  bool curr_right_valid_{false};
  /** Current hash join values iterator */
  decltype(std::vector<HashJoinValue>{}.begin()) iter_;
  /** End iterator */
  decltype(std::vector<HashJoinValue>{}.begin()) end_;

  // for left join
  class LeftHashJoinIterator {
   public:
    LeftHashJoinIterator() = default;
    explicit LeftHashJoinIterator(MapType &hash_map) : hash_iter_(hash_map.begin()), hash_end_(hash_map.end()) {
      if (IsEnd()) {
        return;
      }
      iter_ = hash_iter_->second.tuples_.begin();
      end_ = hash_iter_->second.tuples_.end();
      TranverseToNotEmitted();
    }
    auto operator++() -> LeftHashJoinIterator & {
      ++iter_;
      TranverseToNotEmitted();
      return *this;
    }
    auto operator*() -> Tuple & { return iter_->tuple_; }
    auto IsEnd() -> bool { return hash_iter_ == hash_end_; }

   private:
    auto TranverseToNotEmitted() -> void {
      for (;;) {
        if (iter_ == end_) {
          ++hash_iter_;
          if (IsEnd()) {
            return;
          }
          iter_ = hash_iter_->second.tuples_.begin();
          end_ = hash_iter_->second.tuples_.end();
        }
        if (iter_->is_emitted_) {
          ++iter_;
          continue;
        }
        break;
      }
    }
    /** Hash map iterator */
    decltype(MapType{}.begin()) hash_iter_{};
    decltype(MapType{}.begin()) hash_end_{};
    /** Current left tuples iterator */
    decltype(std::vector<HashJoinValue>{}.begin()) iter_{};
    decltype(std::vector<HashJoinValue>{}.begin()) end_{};
  };

  LeftHashJoinIterator left_iter_{};
  bool right_child_end_{false};
};

}  // namespace bustub
