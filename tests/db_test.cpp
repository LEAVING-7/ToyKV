#include <gtest/gtest.h>

#include "../db.hpp"
#include "ramdom_data.hpp"

auto destroyDB(Database& db)
{
  db.close();
  std::filesystem::remove_all(db.getOption().dirPath);
  std::filesystem::remove_all(mergeDirPath(db.getOption().dirPath));
}

auto getKeyBytes(int i) -> Bytes
{
  auto [data, len] = genTestKey(i);
  return Bytes{len, std::move(data)};
}

auto genValueBytes(int n) -> Bytes
{
  auto [data, len] = randomValue(n);
  return Bytes{len, std::move(data)};
}
