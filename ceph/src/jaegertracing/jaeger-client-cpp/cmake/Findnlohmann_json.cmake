# - Try to find nlohmann_json
#
# The following variables are optionally searched for defaults
#  nlohmann_json_ROOT_DIR:            Base directory where all GLOG components are found
#
# The following are set after configuration is done: 
#  nlohmann_json_FOUND
#  nlohmann_json_INCLUDE_DIRS
#  nlohmann_json_LIBRARIES

include(FindPackageHandleStandardArgs)

# only look in default directories
set(nlohmann_json_INCLUDE_NAME "nlohmann/json.hpp")

find_path(nlohmann_json_INCLUDE_DIR
    NAMES
        nlohmann/json.hpp
    PATHS /usr/local/include
          /usr/include)

if (NOT nlohmann_json_INCLUDE_DIR)
	set(nlohmann_json_INCLUDE_NAME "json.hpp")
	find_path(
		nlohmann_json_INCLUDE_DIR
		NAMES "${nlohmann_json_INCLUDE_NAME}"
	)
endif()


# Version detection. Unfortunately the header doesn't expose a proper version
# define.
if (nlohmann_json_INCLUDE_DIR AND nlohmann_json_INCLUDE_NAME)
	file(READ "${nlohmann_json_INCLUDE_DIR}/${nlohmann_json_INCLUDE_NAME}" NL_HDR_TXT LIMIT 1000)
	if (NL_HDR_TXT MATCHES "version ([0-9]+\.[0-9]+\.[0-9]+)")
		set(nlohmann_json_VERSION "${CMAKE_MATCH_1}")
	endif()
endif()

set(nlohmann_json_VERSION "${nlohmann_json_VERSION}" CACHE STRING "nlohmann header version")

# handle the QUIETLY and REQUIRED arguments and set nlohmann_json_FOUND to TRUE
# if all listed variables are TRUE, hide their existence from configuration view
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
	nlohmann_json
	REQUIRED_VARS nlohmann_json_INCLUDE_DIR nlohmann_json_INCLUDE_NAME
	VERSION_VAR nlohmann_json_VERSION)

if(nlohmann_json_FOUND AND NOT (TARGET nlohmann_json))
  add_library(nlohmann_json SHARED IMPORTED)
  set_target_properties(nlohmann_json PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${nlohmann_json_INCLUDE_DIR}"
  )
endif()
