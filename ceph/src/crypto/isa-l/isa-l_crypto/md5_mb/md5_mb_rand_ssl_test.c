/**********************************************************************
  Copyright(c) 2011-2016 Intel Corporation All rights reserved.

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions
  are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in
      the documentation and/or other materials provided with the
      distribution.
    * Neither the name of Intel Corporation nor the names of its
      contributors may be used to endorse or promote products derived
      from this software without specific prior written permission.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
**********************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <openssl/md5.h>
#include "md5_mb.h"
#include "endian_helper.h"

#define TEST_LEN  (1024*1024)
#define TEST_BUFS 200
#ifndef RANDOMS
# define RANDOMS  10
#endif
#ifndef TEST_SEED
# define TEST_SEED 0x1234
#endif

/* Reference digest global to reduce stack usage */
static uint8_t digest_ssl[TEST_BUFS][4 * MD5_DIGEST_NWORDS];

// Generates pseudo-random data
void rand_buffer(unsigned char *buf, const long buffer_size)
{
	long i;
	for (i = 0; i < buffer_size; i++)
		buf[i] = rand();
}

int main(void)
{
	MD5_HASH_CTX_MGR *mgr = NULL;
	MD5_HASH_CTX ctxpool[TEST_BUFS];
	unsigned char *bufs[TEST_BUFS];
	uint32_t i, j, fail = 0;
	uint32_t lens[TEST_BUFS];
	unsigned int jobs, t;
	int ret;

	printf("multibinary_md5 test, %d sets of %dx%d max: ", RANDOMS, TEST_BUFS, TEST_LEN);

	srand(TEST_SEED);

	ret = posix_memalign((void *)&mgr, 16, sizeof(MD5_HASH_CTX_MGR));
	if ((ret != 0) || (mgr == NULL)) {
		printf("posix_memalign failed test aborted\n");
		return 1;
	}

	md5_ctx_mgr_init(mgr);

	for (i = 0; i < TEST_BUFS; i++) {
		// Allocate and fill buffer
		bufs[i] = (unsigned char *)malloc(TEST_LEN);
		if (bufs[i] == NULL) {
			printf("malloc failed test aborted\n");
			return 1;
		}
		rand_buffer(bufs[i], TEST_LEN);

		// Init ctx contents
		hash_ctx_init(&ctxpool[i]);
		ctxpool[i].user_data = (void *)((uint64_t) i);

		// SSL test
		MD5(bufs[i], TEST_LEN, digest_ssl[i]);

		// sb_md5 test
		md5_ctx_mgr_submit(mgr, &ctxpool[i], bufs[i], TEST_LEN, HASH_ENTIRE);
	}

	while (md5_ctx_mgr_flush(mgr)) ;

	for (i = 0; i < TEST_BUFS; i++) {
		for (j = 0; j < MD5_DIGEST_NWORDS; j++) {
			if (ctxpool[i].job.result_digest[j] !=
			    to_le32(((uint32_t *) digest_ssl[i])[j])) {
				fail++;
				printf("Test%d, digest%d fail %08X <=> %08X\n",
				       i, j, ctxpool[i].job.result_digest[j],
				       to_le32(((uint32_t *) digest_ssl[i])[j]));
			}
		}
	}
	putchar('.');

	// Run tests with random size and number of jobs
	for (t = 0; t < RANDOMS; t++) {
		jobs = rand() % (TEST_BUFS);

		md5_ctx_mgr_init(mgr);

		for (i = 0; i < jobs; i++) {
			// Random buffer with random len and contents
			lens[i] = rand() % (TEST_LEN);
			rand_buffer(bufs[i], lens[i]);

			// Run SSL test
			MD5(bufs[i], lens[i], digest_ssl[i]);

			// Run sb_md5 test
			md5_ctx_mgr_submit(mgr, &ctxpool[i], bufs[i], lens[i], HASH_ENTIRE);
		}

		while (md5_ctx_mgr_flush(mgr)) ;

		for (i = 0; i < jobs; i++) {
			for (j = 0; j < MD5_DIGEST_NWORDS; j++) {
				if (ctxpool[i].job.result_digest[j] !=
				    to_le32(((uint32_t *) digest_ssl[i])[j])) {
					fail++;
					printf("Test%d, digest%d fail %08X <=> %08X\n",
					       i, j, ctxpool[i].job.result_digest[j],
					       to_le32(((uint32_t *) digest_ssl[i])[j]));
				}
			}
		}
		if (fail) {
			printf("Test failed function check %d\n", fail);
			return fail;
		}

		putchar('.');
		fflush(0);
	}			// random test t

	if (fail)
		printf("Test failed function check %d\n", fail);
	else
		printf(" multibinary_md5_ssl rand: Pass\n");

	return fail;
}
