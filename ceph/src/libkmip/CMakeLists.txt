set(libkmip_srcs
  kmip.c
  kmip_memset.c
  kmip_bio.c)

add_library(kmip STATIC ${libkmip_srcs})

target_link_libraries(kmip $<$<PLATFORM_ID:Darwin>:OpenSSL::SSL>)

IF (CMAKE_SYSTEM_NAME STREQUAL "Linux")
  LIST (APPEND SYSFLAGS -D_POSIX_C_SOURCE=1)
ENDIF ()
