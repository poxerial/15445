#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;

  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (bpm_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    bpm_ = nullptr;
    page_ = nullptr;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;

  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
  return *this;
}

BasicPageGuard::~BasicPageGuard(){
    Drop();
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  this->guard_ = std::move(that.guard_);
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  this->guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
  }
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
  Drop();
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  guard_ = std::move(that.guard_);
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
  }
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
  Drop();
}  // NOLINT

}  // namespace bustub
