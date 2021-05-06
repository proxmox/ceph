set(libkmip_srcs
  kmip.c
  kmip_memset.c
  kmip_bio.c)

add_library(kmip STATIC ${libkmip_srcs})

IF (CMAKE_SYSTEM_NAME STREQUAL "Linux")
  LIST (APPEND SYSFLAGS -D_POSIX_C_SOURCE=1)
ENDIF ()
