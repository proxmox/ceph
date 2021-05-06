add_subdirectory(dependency)

add_library(test_base INTERFACE)
target_link_libraries(test_base INTERFACE gtest_main)

# all warnings as errors
if(MSVC)
	target_compile_options(test_base INTERFACE /W4 /WX)
else(MSVC)
	target_compile_options(test_base INTERFACE -Wall -Wextra -Werror)
endif(MSVC)

if(SPAWN_TEST_ADDRESS_SANITIZER)
	# add address sanitizier
	target_compile_options(test_base INTERFACE "-fsanitize=address")
	target_link_libraries(test_base INTERFACE "-fsanitize=address")
endif()

add_executable(test_async_result test_async_result.cc)
target_link_libraries(test_async_result test_base spawn)
add_test(test_async_result test_async_result)

add_executable(test_spawn test_spawn.cc)
target_link_libraries(test_spawn test_base spawn)
add_test(test_spawn test_spawn)

add_executable(test_exception test_exception.cc)
target_link_libraries(test_exception test_base spawn)
add_test(test_exception test_exception)
