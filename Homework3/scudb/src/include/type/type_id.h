#pragma once

namespace scudb {
// Every possible SQL type ID
enum TypeId {
  INVALID = 0,
  BOOLEAN,
  TINYINT,
  SMALLINT,
  INTEGER,
  BIGINT,
  DECIMAL,
  VARCHAR,
  TIMESTAMP,
};
} // namespace scudb
