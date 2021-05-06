// Copyright (c) 2020 Casey Bodley (cbodley at redhat dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#include <spawn/spawn.hpp>

// make assertions about async_result::return_type with different signatures
// this is a compilation test only

template <typename Sig>
struct yield_result : boost::asio::async_result<spawn::yield_context, Sig> {};

template <typename T, typename Sig>
struct yield_returns : std::is_same<T, typename yield_result<Sig>::return_type> {};

// no return value
static_assert(yield_returns<void, void()>::value,
              "wrong return value for void()");
static_assert(yield_returns<void, void(boost::system::error_code)>::value,
              "wrong return value for void(error_code)");
// single-parameter return value
static_assert(yield_returns<int, void(int)>::value,
              "wrong return value for void(int)");
static_assert(yield_returns<int, void(boost::system::error_code, int)>::value,
              "wrong return value for void(error_code, int)");
// multiple-parameter return value
static_assert(yield_returns<std::tuple<int, std::string>,
                            void(int, std::string)>::value,
              "wrong return value for void(int, string)");
static_assert(yield_returns<std::tuple<int, std::string>,
                            void(boost::system::error_code, int, std::string)>::value,
              "wrong return value for void(error_code, int, string)");
// single-tuple-parameter return value
static_assert(yield_returns<std::tuple<int, std::string>,
                            void(std::tuple<int, std::string>)>::value,
              "wrong return value for void(std::tuple<int>)");
static_assert(yield_returns<std::tuple<int, std::string>,
                            void(boost::system::error_code, std::tuple<int, std::string>)>::value,
              "wrong return value for void(error_code, std::tuple<int>)");
// single-pair-parameter return value
static_assert(yield_returns<std::pair<int, std::string>,
                            void(std::pair<int, std::string>)>::value,
              "wrong return value for void(std::tuple<int>)");
static_assert(yield_returns<std::pair<int, std::string>,
                            void(boost::system::error_code, std::pair<int, std::string>)>::value,
              "wrong return value for void(error_code, std::tuple<int>)");
