#include <gtest/gtest.h>

#include "../db.hpp"
#include "ramdom_data.hpp"
auto destroyDB(Database* db)
{
  std::filesystem::remove_all(db->getOption().dirPath);
  std::filesystem::remove_all(mergeDirPath(db->getOption().dirPath));
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

auto genData(Database& db, int start, int end, int valueLen)
{
  for (int i = start; i < end; i++) {
    auto k = getKeyBytes(i);
    auto v = genValueBytes(valueLen);
    auto e = db.put(k, v);
    ASSERT_FALSE(e);
  }
}

auto batchPutAndIterate(std::int64_t segmentSize, int size, int valueLen)
{
  auto opt = DbOption{};
  opt.segmentSize = segmentSize;

  auto r = Database::open(opt);
  if (!r) {
    throw std::system_error(r.error());
  }
  auto db = std::move(r).value();

  auto batch = db->newBatch(BatchOption{});
  auto memMap = std::unordered_map<Bytes, Bytes, BytesHash>{};
  for (int i = 0; i < size; i++) {
    auto k = getKeyBytes(i);
    auto v = genValueBytes(valueLen);
    memMap[k] = v;
    auto e = batch->put(k, v);
    ASSERT_FALSE(e);
  }
  
  auto e = batch->commit();
  ASSERT_FALSE(e);

  for (int i = 0; i < size; i++) {
    auto value = db->get(getKeyBytes(i));
    if (!value) {
      throw std::runtime_error(std::format("key {} not found: {}", i, value.error().message()));
    }
    ASSERT_EQ(*value, memMap[getKeyBytes(i)]);
  }

  db->close();
  auto dr = Database::open(opt);
  auto db2 = std::move(dr).value();
  for (int i = 0; i < size; i++) {
    auto v = db2->get(getKeyBytes(i));
    ASSERT_TRUE(v);
    ASSERT_EQ(*v, memMap[getKeyBytes(i)]);
  }

  destroyDB(db.get());
}

TEST(Batch, PutNormal)
{
  batchPutAndIterate(1 * GiB, 10000, 128);
  // value 1KB
  batchPutAndIterate(1 * GiB, 10000, KiB);
  // value 32KB
  batchPutAndIterate(1 * GiB, 1000, 32 * KiB);
}

TEST(Batch, PutIncSegFile)
{
  batchPutAndIterate(32 * MiB, 5000, 32 * KiB);
  ;
}

TEST(Batch, GetNormal)
{
  auto opt = DbOption{};
  auto r = Database::open(opt);
  if (!r) {
    throw std::system_error(r.error());
  }
  auto db = std::move(r).value();

  auto batch1 = db->newBatch(BatchOption{});
  auto k = getKeyBytes(12);
  auto v = genValueBytes(128);
  auto e = batch1->put(k, v);
  ASSERT_FALSE(e);
  auto val1 = batch1->get(getKeyBytes(12));
  ASSERT_TRUE(val1);
  ASSERT_EQ(*val1, v);
  auto e2 = batch1->commit();
  ASSERT_FALSE(e2);

  genData(*db, 400, 500, 4 * KiB);

  auto batch2 = db->newBatch(BatchOption{});
  auto e3 = batch2->del(getKeyBytes(450));
  ASSERT_FALSE(e3);

  auto val2 = batch2->get(getKeyBytes(450));
  ASSERT_FALSE(val2);
  ASSERT_TRUE(val2.error() == DbErr::KeyNotFound);

  auto e4 = batch2->commit();
  ASSERT_FALSE(e4);

  db->close();

  auto dr = Database::open(opt);
  auto db2 = std::move(dr).value();
  auto val3 = db2->get(getKeyBytes(12));
  ASSERT_TRUE(val3);
  ASSERT_EQ(*val3, v);
  auto val4 = db2->get(getKeyBytes(450));
  ASSERT_FALSE(val4);
  ASSERT_TRUE(val4.error() == DbErr::KeyNotFound);
  db2->close();
  destroyDB(db.get());
}

TEST(Batch, DeleteNormal)
{
  auto opt = DbOption{};
  auto r = Database::open(opt);
  if (!r) {
    throw std::system_error(r.error());
  }
  auto db = std::move(r).value();

  auto e = db->del(Bytes::from("not exist"));
  ASSERT_FALSE(e);

  genData(*db, 1, 100, 128);

  auto e2 = db->del(getKeyBytes(99));
  ASSERT_FALSE(e2);

  auto exists = db->exist(getKeyBytes(99));
  ASSERT_TRUE(exists.has_value());
  ASSERT_FALSE(*exists);

  auto batch = db->newBatch(BatchOption{});
  auto e4 = batch->put(getKeyBytes(200), genValueBytes(100));
  ASSERT_FALSE(e4);
  auto e5 = batch->del(getKeyBytes(200));
  ASSERT_FALSE(e5);

  auto exits2 = batch->exist(getKeyBytes(200));
  ASSERT_TRUE(exits2.has_value());
  ASSERT_FALSE(*exits2);
  batch->commit();

  db->close();
  auto dr = Database::open(opt);
  auto db2 = std::move(dr).value();
  auto exists3 = db2->exist(getKeyBytes(200));
  ASSERT_TRUE(exists3.has_value());
  ASSERT_FALSE(*exists3);
  db2->close();
  destroyDB(db.get());
}

TEST(Batch, ExistNormal)
{
  auto opt = DbOption{};
  auto r = Database::open(opt);
  if (!r) {
    throw std::system_error(r.error());
  }
  auto db = std::move(r).value();

  genData(*db, 1, 100, 128);

  auto batch = db->newBatch(BatchOption{});
  auto exists = batch->exist(getKeyBytes(99));
  ASSERT_TRUE(exists.has_value());
  ASSERT_TRUE(*exists);

  auto exists1 = batch->exist(getKeyBytes(1000));
  ASSERT_TRUE(exists1.has_value());
  ASSERT_FALSE(*exists1);
  batch->commit();

  db->close();

  auto dr = Database::open(opt);
  auto db2 = std::move(dr).value();
  auto exists2 = db2->exist(getKeyBytes(99));
  ASSERT_TRUE(exists2.has_value());
  ASSERT_TRUE(*exists2);
  auto exists3 = db2->exist(getKeyBytes(1000));
  ASSERT_TRUE(exists3.has_value());
  ASSERT_FALSE(*exists3);
  db2->close();
  destroyDB(db.get());
}

TEST(Batch, Rollback)
{
  auto opt = DbOption{};
  auto r = Database::open(opt);
  if (!r) {
    throw std::system_error(r.error());
  }
  auto db = std::move(r).value();

  auto key = Bytes::from("key");
  auto value = Bytes::from("value");

  auto batch = db->newBatch(BatchOption{});
  auto e = batch->put(key, value);
  ASSERT_FALSE(e);
  auto e2 = batch->rollback();
  ASSERT_FALSE(e2);

  auto v = db->get(key);
  ASSERT_FALSE(v);
  ASSERT_TRUE(v.error() == DbErr::KeyNotFound);
  destroyDB(db.get());
}