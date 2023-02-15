/**
 * lock_manager.cpp
 */

#include "concurrency/lock_manager.h"

namespace scudb {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  return false;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  return false;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  return false;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  return false;
}

} // namespace scudb
