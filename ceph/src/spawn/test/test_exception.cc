//
// test_exception.cpp
// ~~~~~~~~~~~~~~~
//
// Copyright (c) 2020 Casey Bodley (cbodley at redhat dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#include <spawn/spawn.hpp>

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <gtest/gtest.h>


struct throwing_handler {
  template <typename T>
  void operator()(spawn::basic_yield_context<T>) {
    throw std::runtime_error("");
  }
};

TEST(Exception, SpawnThrowInHelper)
{
  boost::asio::io_context ioc;
  spawn::spawn(ioc, throwing_handler());
  EXPECT_THROW(ioc.run_one(), std::runtime_error); // spawn->throw
}

struct noop_handler {
  template <typename T>
  void operator()(spawn::basic_yield_context<T>) {}
};

struct throwing_completion_handler {
  void operator()() {
    throw std::runtime_error("");
  }
};

TEST(Exception, SpawnHandlerThrowInHelper)
{
  boost::asio::io_context ioc;
  spawn::spawn(bind_executor(ioc.get_executor(),
                             throwing_completion_handler()),
               noop_handler());
  EXPECT_THROW(ioc.run_one(), std::runtime_error); // spawn->throw
}

template <typename CompletionToken>
auto async_yield(CompletionToken&& token)
  -> BOOST_ASIO_INITFN_RESULT_TYPE(CompletionToken, void())
{
  boost::asio::async_completion<CompletionToken, void()> init(token);
  boost::asio::post(std::move(init.completion_handler));
  return init.result.get();
}

struct yield_throwing_handler {
  template <typename T>
  void operator()(spawn::basic_yield_context<T> y) {
    async_yield(y); // suspend and resume before throwing
    throw std::runtime_error("");
  }
};

TEST(Exception, SpawnThrowAfterYield)
{
  boost::asio::io_context ioc;
  spawn::spawn(ioc, yield_throwing_handler());
  ASSERT_NO_THROW(ioc.run_one()); // yield_throwing_handler suspend
  EXPECT_THROW(ioc.run_one(), std::runtime_error); // resume + throw
}

struct yield_handler {
  template <typename T>
  void operator()(spawn::basic_yield_context<T> y) {
    async_yield(y);
  }
};

TEST(Exception, SpawnHandlerThrowAfterYield)
{
  boost::asio::io_context ioc;
  spawn::spawn(bind_executor(ioc.get_executor(),
                             throwing_completion_handler()),
               yield_handler());
  ASSERT_NO_THROW(ioc.run_one()); // yield_handler suspend
  EXPECT_THROW(ioc.run_one(), std::runtime_error); // resume + throw
}

struct nested_throwing_handler {
  template <typename T>
  void operator()(spawn::basic_yield_context<T> y) {
    spawn::spawn(y, throwing_handler());
  }
};

TEST(Exception, SpawnThrowInNestedHelper)
{
  boost::asio::io_context ioc;
  spawn::spawn(ioc, nested_throwing_handler());
  EXPECT_THROW(ioc.run_one(), std::runtime_error); // spawn->spawn->throw
}

struct yield_nested_throwing_handler {
  template <typename T>
  void operator()(spawn::basic_yield_context<T> y) {
    async_yield(y); // suspend and resume before spawning
    spawn::spawn(y, yield_throwing_handler());
  }
};

TEST(Exception, SpawnThrowAfterNestedYield)
{
  boost::asio::io_context ioc;
  spawn::spawn(ioc, yield_nested_throwing_handler());
  ASSERT_NO_THROW(ioc.run_one()); // yield_nested_throwing_handler suspend
  ASSERT_NO_THROW(ioc.run_one()); // yield_throwing_handler suspend
  EXPECT_THROW(ioc.run_one(), std::runtime_error); // resume + throw
}

struct yield_throw_after_nested_handler {
  template <typename T>
  void operator()(spawn::basic_yield_context<T> y) {
    async_yield(y); // suspend and resume before spawning
    spawn::spawn(y, yield_handler());
    throw std::runtime_error("");
  }
};

TEST(Exception, SpawnThrowAfterNestedSpawn)
{
  boost::asio::io_context ioc;
  spawn::spawn(ioc, yield_throw_after_nested_handler());
  ASSERT_NO_THROW(ioc.run_one()); // yield_throw_after_nested_handler suspend
  EXPECT_THROW(ioc.run_one(), std::runtime_error); // resume + throw
  EXPECT_EQ(1, ioc.poll()); // yield_handler resume
  EXPECT_TRUE(ioc.stopped());
}
