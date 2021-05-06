#.rst:
# FindOpenTracing
# ------------
#
# This module finds the `OpenTracing` library.
#
# Imported target
# ^^^^^^^^^^^^^^^
#
# This module defines the following :prop_tgt:`IMPORTED` target:
#
# ``OpenTracing``
#   The Opentracing library, if found
#
# Result variables
# ^^^^^^^^^^^^^^^^
#
# This module sets the following
#
# ``OpenTracing_FOUND``
#   ``TRUE`` if system has OpenTracing
# ``OpenTracing_INCLUDE_DIRS``
#   The OpenTracing include directories
# ``OpenTracing_LIBRARIES``
#   The libraries needed to use OpenTracing
# ``OpenTracing_VERSION_STRING``
#   The OpenTracing version

#=============================================================================
# Copyright 2018 Mania Abdi, Inc.
# Copyright 2018 Mania Abdi
#
# Distributed under the OSI-approved BSD License (the "License");
# see accompanying file Copyright.txt for details.
#
# This software is distributed WITHOUT ANY WARRANTY; without even the
# implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the License for more information.
#=============================================================================
# (To distribute this file outside of CMake, substitute the full
#  License text for the above reference.)

find_path(OpenTracing_INCLUDE_DIRS
  NAMES  opentracing/tracer.h
  HINTS
    ${OpenTracing_HOME}
    ENV OpenTracing_HOME
  PATH_SUFFIXES
      include
  )
message(STATUS "OpenTracing_INCLUDE_DIRS ${OpenTracing_INCLUDE_DIRS}")
find_library(OpenTracing_LIBRARIES NAMES
  opentracing libopentracing
  HINTS
    ${OpenTracing_HOME}
    ENV OpenTracing_HOME
  PATH_SUFFIXES
      lib lib64
  )
message(STATUS "opentracing libraries ${OpenTracing_LIBRARIES}")

if(OpenTracing_INCLUDE_DIRS AND OpenTracing_LIBRARIES)

  # will need specifically 1.5.x for successful working with Jaeger
  set(OpenTracing_VERSION_STRING "1.6.0")

  if(NOT TARGET OpenTracing::opentracing)
    add_library(OpenTracing::opentracing SHARED IMPORTED)
    set_target_properties(OpenTracing::opentracing PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${OpenTracing_INCLUDE_DIRS}"
      INTERFACE_LINK_LIBRARIES ${CMAKE_DL_LIBS}
      IMPORTED_LINK_INTERFACE_LANGUAGES "C"
      IMPORTED_LOCATION "${OpenTracing_LIBRARIES}")
  endif()

  if(NOT TARGET OpenTracing::opentracing-static)
    add_library(OpenTracing::opentracing-static STATIC IMPORTED)
    set_target_properties(OpenTracing::opentracing-static PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${OpenTracing_INCLUDE_DIRS}"
      INTERFACE_LINK_LIBRARIES ${CMAKE_DL_LIBS}
      IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
      IMPORTED_LOCATION "${OpenTracing_LIBRARIES}")
  endif()
  message(STATUS "include opentracing ${OpenTracing_INCLUDE_DIRS}")

  # add libdl to required libraries
  set(OpenTracing_LIBRARIES ${OpenTracing_LIBRARIES} ${CMAKE_DL_LIBS})
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(OpenTracing FOUND_VAR OpenTracing_FOUND
                                  REQUIRED_VARS OpenTracing_LIBRARIES
                                                OpenTracing_INCLUDE_DIRS
                                  VERSION_VAR OpenTracing_VERSION_STRING)
mark_as_advanced(OpenTracing_LIBRARIES OpenTracing_INCLUDE_DIRS)
