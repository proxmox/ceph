//
// spawn.cpp
// ~~~~~~~~~~~~~~~
//
// Copyright (c) 2003-2019 Christopher M. Kohlhoff (chris at kohlhoff dot com)
// Copyright (c) 2019 Casey Bodley (cbodley at redhat dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

// Test that header file is self-contained.
#include <spawn/spawn.hpp>

#include <boost/asio/io_context.hpp>
#include <boost/asio/system_timer.hpp>
#include <boost/context/protected_fixedsize_stack.hpp>
#include <boost/optional.hpp>
#include <gtest/gtest.h>


boost::context::protected_fixedsize_stack with_stack_allocator()
{
  return boost::context::protected_fixedsize_stack(65536);
}

struct counting_handler {
  int& count;
  counting_handler(int& count) : count(count) {}
  void operator()() { ++count; }
  template <typename T>
  void operator()(spawn::basic_yield_context<T>) { ++count; }
};

TEST(Spawn, SpawnFunction)
{
  boost::asio::io_context ioc;
  int called = 0;
  spawn::spawn(counting_handler(called));
  ASSERT_EQ(0, ioc.run()); // runs in system executor
  ASSERT_TRUE(ioc.stopped());
  ASSERT_EQ(1, called);
}

TEST(Spawn, SpawnBoundFunction)
{
  boost::asio::io_context ioc;
  int called = 0;
  spawn::spawn(bind_executor(ioc.get_executor(), counting_handler(called)));
  ASSERT_EQ(1, ioc.run());
  ASSERT_TRUE(ioc.stopped());
  ASSERT_EQ(1, called);
}

TEST(Spawn, SpawnFunctionStackAllocator)
{
  boost::asio::io_context ioc;
  int called = 0;
  spawn::spawn(counting_handler(called),
               with_stack_allocator());
  ASSERT_EQ(0, ioc.run());
  ASSERT_TRUE(ioc.stopped());
  ASSERT_EQ(1, called);
}

TEST(Spawn, SpawnHandler)
{
  boost::asio::io_context ioc;
  boost::asio::strand<boost::asio::io_context::executor_type> strand(ioc.get_executor());
  int called = 0;
  spawn::spawn(bind_executor(strand, counting_handler(called)),
               counting_handler(called));
  ASSERT_EQ(1, ioc.run());
  ASSERT_TRUE(ioc.stopped());
  ASSERT_EQ(2, called);
}

TEST(Spawn, SpawnHandlerStackAllocator)
{
  boost::asio::io_context ioc;
  typedef boost::asio::io_context::executor_type executor_type;
  boost::asio::strand<executor_type> strand(ioc.get_executor());
  int called = 0;
  spawn::spawn(bind_executor(strand, counting_handler(called)),
               counting_handler(called),
               with_stack_allocator());
  ASSERT_EQ(1, ioc.run());
  ASSERT_TRUE(ioc.stopped());
  ASSERT_EQ(2, called);
}

struct spawn_counting_handler {
  int& count;
  spawn_counting_handler(int& count) : count(count) {}
  template <typename T>
  void operator()(spawn::basic_yield_context<T> y)
  {
    spawn::spawn(y, counting_handler(count));
    ++count;
  }
};

TEST(Spawn, SpawnYieldContext)
{
  boost::asio::io_context ioc;
  int called = 0;
  spawn::spawn(bind_executor(ioc.get_executor(),
                             counting_handler(called)),
               spawn_counting_handler(called));
  ASSERT_EQ(1, ioc.run());
  ASSERT_TRUE(ioc.stopped());
  ASSERT_EQ(3, called);
}

struct spawn_alloc_counting_handler {
  int& count;
  spawn_alloc_counting_handler(int& count) : count(count) {}
  template <typename T>
  void operator()(spawn::basic_yield_context<T> y)
  {
    spawn::spawn(y, counting_handler(count),
                       with_stack_allocator());
    ++count;
  }
};

TEST(Spawn, SpawnYieldContextStackAllocator)
{
  boost::asio::io_context ioc;
  int called = 0;
  spawn::spawn(bind_executor(ioc.get_executor(),
                             counting_handler(called)),
               spawn_alloc_counting_handler(called));
  ASSERT_EQ(1, ioc.run());
  ASSERT_TRUE(ioc.stopped());
  ASSERT_EQ(3, called);
}

TEST(Spawn, SpawnExecutor)
{
  boost::asio::io_context ioc;
  int called = 0;
  spawn::spawn(ioc.get_executor(), counting_handler(called));
  ASSERT_EQ(1, ioc.run());
  ASSERT_TRUE(ioc.stopped());
  ASSERT_EQ(1, called);
}

TEST(Spawn, SpawnExecutorStackAllocator)
{
  boost::asio::io_context ioc;
  int called = 0;
  spawn::spawn(ioc.get_executor(),
               counting_handler(called),
               with_stack_allocator());
  ASSERT_EQ(1, ioc.run());
  ASSERT_TRUE(ioc.stopped());
  ASSERT_EQ(1, called);
}

TEST(Spawn, SpawnStrand)
{
  boost::asio::io_context ioc;
  typedef boost::asio::io_context::executor_type executor_type;
  int called = 0;
  spawn::spawn(boost::asio::strand<executor_type>(ioc.get_executor()),
               counting_handler(called));
  ASSERT_EQ(1, ioc.run());
  ASSERT_TRUE(ioc.stopped());
  ASSERT_EQ(1, called);
}

TEST(Spawn, SpawnStrandStackAllocator)
{
  boost::asio::io_context ioc;
  typedef boost::asio::io_context::executor_type executor_type;
  int called = 0;
  spawn::spawn(boost::asio::strand<executor_type>(ioc.get_executor()),
               counting_handler(called),
               with_stack_allocator());
  ASSERT_EQ(1, ioc.run());
  ASSERT_TRUE(ioc.stopped());
  ASSERT_EQ(1, called);
}

TEST(Spawn, SpawnExecutionContext)
{
  boost::asio::io_context ioc;
  int called = 0;
  spawn::spawn(ioc, counting_handler(called));
  ASSERT_EQ(1, ioc.run());
  ASSERT_TRUE(ioc.stopped());
  ASSERT_EQ(1, called);
}

TEST(Spawn, SpawnExecutionContextStackAllocator)
{
  boost::asio::io_context ioc;
  int called = 0;
  spawn::spawn(ioc, counting_handler(called),
               with_stack_allocator());
  ASSERT_EQ(1, ioc.run());
  ASSERT_TRUE(ioc.stopped());
  ASSERT_EQ(1, called);
}

typedef boost::asio::system_timer timer_type;

struct spawn_wait_handler {
  timer_type& timer;
  spawn_wait_handler(timer_type& timer) : timer(timer) {}
  template <typename T>
  void operator()(spawn::basic_yield_context<T> yield)
  {
    timer.async_wait(yield);
  }
};

TEST(Spawn, SpawnTimer)
{
  int called = 0;
  {
    boost::asio::io_context ioc;
    timer_type timer(ioc, boost::asio::chrono::hours(0));

    spawn::spawn(bind_executor(ioc.get_executor(),
                               counting_handler(called)),
                 spawn_wait_handler(timer));
    ASSERT_EQ(2, ioc.run());
    ASSERT_TRUE(ioc.stopped());
  }
  ASSERT_EQ(1, called);
}

TEST(Spawn, SpawnTimerDestruct)
{
  int called = 0;
  {
    boost::asio::io_context ioc;
    timer_type timer(ioc, boost::asio::chrono::hours(65536));

    spawn::spawn(bind_executor(ioc.get_executor(),
                               counting_handler(called)),
                 spawn_wait_handler(timer));
    ASSERT_EQ(1, ioc.run_one());
    ASSERT_TRUE(!ioc.stopped());
  }
  ASSERT_EQ(0, called);
}

using boost::system::error_code;

template <typename Handler, typename ...Args>
void post(Handler& h, Args&& ...args)
{
  auto ex = boost::asio::get_associated_executor(h);
  auto a = boost::asio::get_associated_allocator(h);
  auto b = std::bind(std::move(h), std::forward<Args>(args)...);
  ex.post(std::move(b), a);
}

struct single_tuple_handler {
  std::tuple<int, std::string>& result;
  void operator()(spawn::yield_context y) {
    using Signature = void(std::tuple<int, std::string>);
    boost::asio::async_completion<spawn::yield_context, Signature> init(y);
    post(init.completion_handler, std::make_tuple(42, std::string{"test"}));
    result = init.result.get();
  }
};

TEST(Spawn, ReturnSingleTuple)
{
  boost::asio::io_context ioc;
  std::tuple<int, std::string> result;
  spawn::spawn(ioc, single_tuple_handler{result});
  ASSERT_EQ(2, ioc.poll());
  EXPECT_EQ(std::make_tuple(42, std::string{"test"}), result);
}

struct multiple2_handler {
  std::tuple<int, std::string>& result;
  void operator()(spawn::yield_context y) {
    using Signature = void(error_code, int, std::string);
    boost::asio::async_completion<spawn::yield_context, Signature> init(y);
    post(init.completion_handler, error_code{}, 42, std::string{"test"});
    result = init.result.get();
  }
};

TEST(Spawn, ReturnMultiple2)
{
  boost::asio::io_context ioc;
  std::tuple<int, std::string> result;
  spawn::spawn(ioc, multiple2_handler{result});
  ASSERT_EQ(2, ioc.poll());
  EXPECT_EQ(std::make_tuple(42, std::string{"test"}), result);
}

struct multiple2_with_moveonly_handler {
  std::tuple<int, std::unique_ptr<int>>& result;
  void operator()(spawn::yield_context y) {
    using Signature = void(int, std::unique_ptr<int>);
    boost::asio::async_completion<spawn::yield_context, Signature> init(y);
    std::unique_ptr<int> ptr{new int(42)};
    init.completion_handler(42, std::move(ptr));
    result = init.result.get();
  }
};

TEST(Spawn, ReturnMultiple2MoveOnly)
{
  boost::asio::io_context ioc;
  std::tuple<int, std::unique_ptr<int>> result;
  spawn::spawn(ioc, multiple2_with_moveonly_handler{result});
  ASSERT_EQ(1, ioc.poll());
  EXPECT_EQ(42, std::get<0>(result));
  ASSERT_TRUE(std::get<1>(result));
  EXPECT_EQ(42, *std::get<1>(result));
}

struct multiple3_handler {
  std::tuple<int, std::string, double>& result;
  void operator()(spawn::yield_context y) {
    using Signature = void(error_code, int, std::string, double);
    boost::asio::async_completion<spawn::yield_context, Signature> init(y);
    post(init.completion_handler, error_code{}, 42, std::string{"test"}, 2.0);
    result = init.result.get();
  }
};

TEST(Spawn, ReturnMultiple3)
{
  boost::asio::io_context ioc;
  std::tuple<int, std::string, double> result;
  spawn::spawn(ioc, multiple3_handler{result});
  ASSERT_EQ(2, ioc.poll());
  EXPECT_EQ(std::make_tuple(42, std::string{"test"}, 2.0), result);
}

struct non_default_constructible {
  non_default_constructible() = delete;
  non_default_constructible(std::nullptr_t) {}
};

struct non_default_constructible_handler {
  boost::optional<non_default_constructible>& result;
  void operator()(spawn::yield_context y) {
    using Signature = void(non_default_constructible);
    boost::asio::async_completion<spawn::yield_context, Signature> init(y);
    post(init.completion_handler, non_default_constructible{nullptr});
    result = init.result.get();
  }
};

TEST(Spawn, ReturnNonDefaultConstructible)
{
  boost::asio::io_context ioc;
  boost::optional<non_default_constructible> result;
  spawn::spawn(ioc, non_default_constructible_handler{result});
  ASSERT_EQ(2, ioc.poll());
  ASSERT_TRUE(result);
}

struct multiple_non_default_constructible_handler {
  boost::optional<std::tuple<int, non_default_constructible>>& result;
  void operator()(spawn::yield_context y) {
    using Signature = void(error_code, int, non_default_constructible);
    boost::asio::async_completion<spawn::yield_context, Signature> init(y);
    post(init.completion_handler, error_code{}, 42,
         non_default_constructible{nullptr});
    result = init.result.get();
  }
};

TEST(Spawn, ReturnMultipleNonDefaultConstructible)
{
  boost::asio::io_context ioc;
  boost::optional<std::tuple<int, non_default_constructible>> result;
  spawn::spawn(ioc, multiple_non_default_constructible_handler{result});
  ASSERT_EQ(2, ioc.poll());
  ASSERT_TRUE(result);
}
