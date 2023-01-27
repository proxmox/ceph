// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include <iterator>
#include <map>
#include <set>
#include <boost/scoped_ptr.hpp>
#include <sys/types.h>
#include <cstdlib>

#include "include/buffer.h"
#include "common/map_cacher.hpp"
#include "osd/SnapMapper.h"
#include "common/Cond.h"

#include "gtest/gtest.h"

using namespace std;

template <typename T>
typename T::iterator rand_choose(T &cont) {
  if (std::empty(cont)) {
    return std::end(cont);
  }
  return std::next(std::begin(cont), rand() % cont.size());
}

string random_string(size_t size)
{
  string name;
  for (size_t j = 0; j < size; ++j) {
    name.push_back('a' + (rand() % 26));
  }
  return name;
}

class PausyAsyncMap : public MapCacher::StoreDriver<string, bufferlist> {
  struct _Op {
    virtual void operate(map<string, bufferlist> *store) = 0;
    virtual ~_Op() {}
  };
  typedef std::shared_ptr<_Op> Op;
  struct Remove : public _Op {
    set<string> to_remove;
    explicit Remove(const set<string> &to_remove) : to_remove(to_remove) {}
    void operate(map<string, bufferlist> *store) override {
      for (set<string>::iterator i = to_remove.begin();
	   i != to_remove.end();
	   ++i) {
	store->erase(*i);
      }
    }
  };
  struct Insert : public _Op {
    map<string, bufferlist> to_insert;
    explicit Insert(const map<string, bufferlist> &to_insert) : to_insert(to_insert) {}
    void operate(map<string, bufferlist> *store) override {
      for (map<string, bufferlist>::iterator i = to_insert.begin();
	   i != to_insert.end();
	   ++i) {
	store->erase(i->first);
	store->insert(*i);
      }
    }
  };
  struct Callback : public _Op {
    Context *context;
    explicit Callback(Context *c) : context(c) {}
    void operate(map<string, bufferlist> *store) override {
      context->complete(0);
    }
  };
public:
  class Transaction : public MapCacher::Transaction<string, bufferlist> {
    friend class PausyAsyncMap;
    list<Op> ops;
    list<Op> callbacks;
  public:
    void set_keys(const map<string, bufferlist> &i) override {
      ops.push_back(Op(new Insert(i)));
    }
    void remove_keys(const set<string> &r) override {
      ops.push_back(Op(new Remove(r)));
    }
    void add_callback(Context *c) override {
      callbacks.push_back(Op(new Callback(c)));
    }
  };
private:

  ceph::mutex lock = ceph::make_mutex("PausyAsyncMap");
  map<string, bufferlist> store;

  class Doer : public Thread {
    static const size_t MAX_SIZE = 100;
    PausyAsyncMap *parent;
    ceph::mutex lock = ceph::make_mutex("Doer lock");
    ceph::condition_variable cond;
    int stopping;
    bool paused;
    list<Op> queue;
  public:
    explicit Doer(PausyAsyncMap *parent) :
      parent(parent), stopping(0), paused(false) {}
    void *entry() override {
      while (1) {
	list<Op> ops;
	{
	  std::unique_lock l{lock};
	  cond.wait(l, [this] {
            return stopping || (!queue.empty() && !paused);
	  });
	  if (stopping && queue.empty()) {
	    stopping = 2;
	    cond.notify_all();
	    return 0;
	  }
	  ceph_assert(!queue.empty());
	  ceph_assert(!paused);
	  ops.swap(queue);
	  cond.notify_all();
	}
	ceph_assert(!ops.empty());

	for (list<Op>::iterator i = ops.begin();
	     i != ops.end();
	     ops.erase(i++)) {
	  if (!(rand()%3))
	    usleep(1+(rand() % 5000));
	  std::lock_guard l{parent->lock};
	  (*i)->operate(&(parent->store));
	}
      }
    }

    void pause() {
      std::lock_guard l{lock};
      paused = true;
      cond.notify_all();
    }

    void resume() {
      std::lock_guard l{lock};
      paused = false;
      cond.notify_all();
    }

    void submit(list<Op> &in) {
      std::unique_lock l{lock};
      cond.wait(l, [this] { return queue.size() < MAX_SIZE;});
      queue.splice(queue.end(), in, in.begin(), in.end());
      cond.notify_all();
    }

    void stop() {
      std::unique_lock l{lock};
      stopping = 1;
      cond.notify_all();
      cond.wait(l, [this] { return stopping == 2; });
      cond.notify_all();
    }
  } doer;

public:
  PausyAsyncMap() : doer(this) {
    doer.create("doer");
  }
  ~PausyAsyncMap() override {
    doer.join();
  }
  int get_keys(
    const set<string> &keys,
    map<string, bufferlist> *out) override {
    std::lock_guard l{lock};
    for (set<string>::const_iterator i = keys.begin();
	 i != keys.end();
	 ++i) {
      map<string, bufferlist>::iterator j = store.find(*i);
      if (j != store.end())
	out->insert(*j);
    }
    return 0;
  }
  int get_next(
    const string &key,
    pair<string, bufferlist> *next) override {
    std::lock_guard l{lock};
    map<string, bufferlist>::iterator j = store.upper_bound(key);
    if (j != store.end()) {
      if (next)
	*next = *j;
      return 0;
    } else {
      return -ENOENT;
    }
  }
  void submit(Transaction *t) {
    doer.submit(t->ops);
    doer.submit(t->callbacks);
  }

  void flush() {
    ceph::mutex lock = ceph::make_mutex("flush lock");
    ceph::condition_variable cond;
    bool done = false;

    class OnFinish : public Context {
      ceph::mutex *lock;
      ceph::condition_variable *cond;
      bool *done;
    public:
      OnFinish(ceph::mutex *lock, ceph::condition_variable *cond, bool *done)
	: lock(lock), cond(cond), done(done) {}
      void finish(int) override {
	std::lock_guard l{*lock};
	*done = true;
	cond->notify_all();
      }
    };
    Transaction t;
    t.add_callback(new OnFinish(&lock, &cond, &done));
    submit(&t);
    {
      std::unique_lock l{lock};
      cond.wait(l, [&] { return done; });
    }
  }

  void pause() {
    doer.pause();
  }
  void resume() {
    doer.resume();
  }
  void stop() {
    doer.stop();
  }

};

class MapCacherTest : public ::testing::Test {
protected:
  boost::scoped_ptr< PausyAsyncMap > driver;
  boost::scoped_ptr<MapCacher::MapCacher<string, bufferlist> > cache;
  map<string, bufferlist> truth;
  set<string> names;
public:
  void assert_bl_eq(bufferlist &bl1, bufferlist &bl2) {
    ASSERT_EQ(bl1.length(), bl2.length());
    bufferlist::iterator j = bl2.begin();
    for (bufferlist::iterator i = bl1.begin();
	 !i.end();
	 ++i, ++j) {
      ASSERT_TRUE(!j.end());
      ASSERT_EQ(*i, *j);
    }
  }
  void assert_bl_map_eq(map<string, bufferlist> &m1,
			map<string, bufferlist> &m2) {
    ASSERT_EQ(m1.size(), m2.size());
    map<string, bufferlist>::iterator j = m2.begin();
    for (map<string, bufferlist>::iterator i = m1.begin();
	 i != m1.end();
	 ++i, ++j) {
      ASSERT_TRUE(j != m2.end());
      ASSERT_EQ(i->first, j->first);
      assert_bl_eq(i->second, j->second);
    }

  }
  size_t random_num() {
    return random() % 10;
  }
  size_t random_size() {
    return random() % 1000;
  }
  void random_bl(size_t size, bufferlist *bl) {
    for (size_t i = 0; i < size; ++i) {
      bl->append(rand());
    }
  }
  void do_set() {
    size_t set_size = random_num();
    map<string, bufferlist> to_set;
    for (size_t i = 0; i < set_size; ++i) {
      bufferlist bl;
      random_bl(random_size(), &bl);
      string key = *rand_choose(names);
      to_set.insert(
	make_pair(key, bl));
    }
    for (map<string, bufferlist>::iterator i = to_set.begin();
	 i != to_set.end();
	 ++i) {
      truth.erase(i->first);
      truth.insert(*i);
    }
    {
      PausyAsyncMap::Transaction t;
      cache->set_keys(to_set, &t);
      driver->submit(&t);
    }
  }
  void remove() {
    size_t remove_size = random_num();
    set<string> to_remove;
    for (size_t i = 0; i < remove_size ; ++i) {
      to_remove.insert(*rand_choose(names));
    }
    for (set<string>::iterator i = to_remove.begin();
	 i != to_remove.end();
	 ++i) {
      truth.erase(*i);
    }
    {
      PausyAsyncMap::Transaction t;
      cache->remove_keys(to_remove, &t);
      driver->submit(&t);
    }
  }
  void get() {
    set<string> to_get;
    size_t get_size = random_num();
    for (size_t i = 0; i < get_size; ++i) {
      to_get.insert(*rand_choose(names));
    }

    map<string, bufferlist> got_truth;
    for (set<string>::iterator i = to_get.begin();
	 i != to_get.end();
	 ++i) {
      map<string, bufferlist>::iterator j = truth.find(*i);
      if (j != truth.end())
	got_truth.insert(*j);
    }

    map<string, bufferlist> got;
    cache->get_keys(to_get, &got);

    assert_bl_map_eq(got, got_truth);
  }

  void get_next() {
    string cur;
    while (true) {
      pair<string, bufferlist> next;
      int r = cache->get_next(cur, &next);

      pair<string, bufferlist> next_truth;
      map<string, bufferlist>::iterator i = truth.upper_bound(cur);
      int r_truth = (i == truth.end()) ? -ENOENT : 0;
      if (i != truth.end())
	next_truth = *i;

      ASSERT_EQ(r, r_truth);
      if (r == -ENOENT)
	break;

      ASSERT_EQ(next.first, next_truth.first);
      assert_bl_eq(next.second, next_truth.second);
      cur = next.first;
    }
  }
  void SetUp() override {
    driver.reset(new PausyAsyncMap());
    cache.reset(new MapCacher::MapCacher<string, bufferlist>(driver.get()));
    names.clear();
    truth.clear();
    size_t names_size(random_num() + 10);
    for (size_t i = 0; i < names_size; ++i) {
      names.insert(random_string(1 + (random_size() % 10)));
    }
  }
  void TearDown() override {
    driver->stop();
    cache.reset();
    driver.reset();
  }

};

TEST_F(MapCacherTest, Simple)
{
  driver->pause();
  map<string, bufferlist> truth;
  set<string> truth_keys;
  string blah("asdf");
  bufferlist bl;
  encode(blah, bl);
  truth[string("asdf")] = bl;
  truth_keys.insert(truth.begin()->first);
  {
    PausyAsyncMap::Transaction t;
    cache->set_keys(truth, &t);
    driver->submit(&t);
    cache->set_keys(truth, &t);
    driver->submit(&t);
  }

  map<string, bufferlist> got;
  cache->get_keys(truth_keys, &got);
  assert_bl_map_eq(got, truth);

  driver->resume();
  sleep(1);

  got.clear();
  cache->get_keys(truth_keys, &got);
  assert_bl_map_eq(got, truth);
}

TEST_F(MapCacherTest, Random)
{
  for (size_t i = 0; i < 5000; ++i) {
    if (!(i % 50)) {
      std::cout << "On iteration " << i << std::endl;
    }
    switch (rand() % 4) {
    case 0:
      get();
      break;
    case 1:
      do_set();
      break;
    case 2:
      get_next();
      break;
    case 3:
      remove();
      break;
    }
  }
}

class MapperVerifier {
  PausyAsyncMap *driver;
  boost::scoped_ptr< SnapMapper > mapper;
  map<snapid_t, set<hobject_t> > snap_to_hobject;
  map<hobject_t, set<snapid_t>> hobject_to_snap;
  snapid_t next;
  uint32_t mask;
  uint32_t bits;
  ceph::mutex lock = ceph::make_mutex("lock");
public:

  MapperVerifier(
    PausyAsyncMap *driver,
    uint32_t mask,
    uint32_t bits)
    : driver(driver),
      mapper(new SnapMapper(g_ceph_context, driver, mask, bits, 0, shard_id_t(1))),
             mask(mask), bits(bits) {}

  hobject_t random_hobject() {
    return hobject_t(
      random_string(1+(rand() % 16)),
      random_string(1+(rand() % 16)),
      snapid_t(rand() % 1000),
      (rand() & ((~0)<<bits)) | (mask & ~((~0)<<bits)),
      0, random_string(rand() % 16));
  }

  void choose_random_snaps(int num, set<snapid_t> *snaps) {
    ceph_assert(snaps);
    ceph_assert(!snap_to_hobject.empty());
    for (int i = 0; i < num || snaps->empty(); ++i) {
      snaps->insert(rand_choose(snap_to_hobject)->first);
    }
  }

  void create_snap() {
    snap_to_hobject[next];
    ++next;
  }

  void create_object() {
    std::lock_guard l{lock};
    if (snap_to_hobject.empty())
      return;
    hobject_t obj;
    do {
      obj = random_hobject();
    } while (hobject_to_snap.count(obj));

    set<snapid_t> &snaps = hobject_to_snap[obj];
    choose_random_snaps(1 + (rand() % 20), &snaps);
    for (set<snapid_t>::iterator i = snaps.begin();
	 i != snaps.end();
	 ++i) {
      map<snapid_t, set<hobject_t> >::iterator j = snap_to_hobject.find(*i);
      ceph_assert(j != snap_to_hobject.end());
      j->second.insert(obj);
    }
    {
      PausyAsyncMap::Transaction t;
      mapper->add_oid(obj, snaps, &t);
      driver->submit(&t);
    }
  }

  std::pair<std::string, ceph::buffer::list> to_raw(
    const std::pair<snapid_t, hobject_t> &to_map) {
    return mapper->to_raw(to_map);
  }

  std::string to_legacy_raw_key(
    const std::pair<snapid_t, hobject_t> &to_map) {
    return mapper->to_legacy_raw_key(to_map);
  }

  std::string to_raw_key(
    const std::pair<snapid_t, hobject_t> &to_map) {
    return mapper->to_raw_key(to_map);
  }

  void trim_snap() {
    std::lock_guard l{lock};
    if (snap_to_hobject.empty())
      return;
    map<snapid_t, set<hobject_t> >::iterator snap =
      rand_choose(snap_to_hobject);
    set<hobject_t> hobjects = snap->second;

    vector<hobject_t> hoids;
    while (mapper->get_next_objects_to_trim(
	     snap->first, rand() % 5 + 1, &hoids) == 0) {
      for (auto &&hoid: hoids) {
	ceph_assert(!hoid.is_max());
	ceph_assert(hobjects.count(hoid));
	hobjects.erase(hoid);

	map<hobject_t, set<snapid_t>>::iterator j =
	  hobject_to_snap.find(hoid);
	ceph_assert(j->second.count(snap->first));
	set<snapid_t> old_snaps(j->second);
	j->second.erase(snap->first);

	{
	  PausyAsyncMap::Transaction t;
	  mapper->update_snaps(
	    hoid,
	    j->second,
	    &old_snaps,
	    &t);
	  driver->submit(&t);
	}
	if (j->second.empty()) {
	  hobject_to_snap.erase(j);
	}
	hoid = hobject_t::get_max();
      }
      hoids.clear();
    }
    ceph_assert(hobjects.empty());
    snap_to_hobject.erase(snap);
  }

  void remove_oid() {
    std::lock_guard l{lock};
    if (hobject_to_snap.empty())
      return;
    map<hobject_t, set<snapid_t>>::iterator obj =
      rand_choose(hobject_to_snap);
    for (set<snapid_t>::iterator i = obj->second.begin();
	 i != obj->second.end();
	 ++i) {
      map<snapid_t, set<hobject_t> >::iterator j =
	snap_to_hobject.find(*i);
      ceph_assert(j->second.count(obj->first));
      j->second.erase(obj->first);
    }
    {
      PausyAsyncMap::Transaction t;
      mapper->remove_oid(
	obj->first,
	&t);
      driver->submit(&t);
    }
    hobject_to_snap.erase(obj);
  }

  void check_oid() {
    std::lock_guard l{lock};
    if (hobject_to_snap.empty())
      return;
    map<hobject_t, set<snapid_t>>::iterator obj =
      rand_choose(hobject_to_snap);
    set<snapid_t> snaps;
    int r = mapper->get_snaps(obj->first, &snaps);
    ceph_assert(r == 0);
    ASSERT_EQ(snaps, obj->second);
  }
};

class SnapMapperTest : public ::testing::Test {
protected:
  boost::scoped_ptr< PausyAsyncMap > driver;
  map<pg_t, std::shared_ptr<MapperVerifier> > mappers;
  uint32_t pgnum;

  void SetUp() override {
    driver.reset(new PausyAsyncMap());
    pgnum = 0;
  }

  void TearDown() override {
    driver->stop();
    mappers.clear();
    driver.reset();
  }

  MapperVerifier &get_tester() {
    //return *(mappers.begin()->second);
    return *(rand_choose(mappers)->second);
  }

  void init(uint32_t to_set) {
    pgnum = to_set;
    for (uint32_t i = 0; i < pgnum; ++i) {
      pg_t pgid(i, 0);
      mappers[pgid].reset(
	new MapperVerifier(
	  driver.get(),
	  i,
	  pgid.get_split_bits(pgnum)
	  )
	);
    }
  }

  void run() {
    for (int i = 0; i < 5000; ++i) {
      if (!(i % 50))
	std::cout << i << std::endl;
      switch (rand() % 5) {
      case 0:
	get_tester().create_snap();
	break;
      case 1:
	get_tester().create_object();
	break;
      case 2:
	get_tester().trim_snap();
	break;
      case 3:
	get_tester().check_oid();
	break;
      case 4:
	get_tester().remove_oid();
	break;
      }
    }
  }
};

TEST_F(SnapMapperTest, Simple) {
  init(1);
  get_tester().create_snap();
  get_tester().create_object();
  get_tester().trim_snap();
}

TEST_F(SnapMapperTest, More) {
  init(1);
  run();
}

TEST_F(SnapMapperTest, MultiPG) {
  init(50);
  run();
}

TEST_F(SnapMapperTest, LegacyKeyConvertion) {
    init(1);
    auto obj = get_tester().random_hobject();
    snapid_t snapid = random() % 10;
    auto snap_obj = make_pair(snapid, obj);
    auto raw = get_tester().to_raw(snap_obj);
    std::string old_key = get_tester().to_legacy_raw_key(snap_obj);
    std::string converted_key =
      SnapMapper::convert_legacy_key(old_key, raw.second);
    std::string new_key = get_tester().to_raw_key(snap_obj);
    std::cout << "Converted: " << old_key << "\nTo:        " << converted_key
	      << "\nNew key:   " << new_key << std::endl;
    ASSERT_EQ(converted_key, new_key);
}

