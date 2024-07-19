/***************************************************************************
 *
 * This file is provided under a dual BSD/GPLv2 license.  When using or
 *   redistributing this file, you may do so under either license.
 * 
 *   GPL LICENSE SUMMARY
 * 
 *   Copyright(c) 2007-2022 Intel Corporation. All rights reserved.
 * 
 *   This program is free software; you can redistribute it and/or modify
 *   it under the terms of version 2 of the GNU General Public License as
 *   published by the Free Software Foundation.
 * 
 *   This program is distributed in the hope that it will be useful, but
 *   WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *   General Public License for more details.
 * 
 *   You should have received a copy of the GNU General Public License
 *   along with this program; if not, write to the Free Software
 *   Foundation, Inc., 51 Franklin St - Fifth Floor, Boston, MA 02110-1301 USA.
 *   The full GNU General Public License is included in this distribution
 *   in the file called LICENSE.GPL.
 * 
 *   Contact Information:
 *   Intel Corporation
 * 
 *   BSD LICENSE
 * 
 *   Copyright(c) 2007-2022 Intel Corporation. All rights reserved.
 *   All rights reserved.
 * 
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 * 
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 * 
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 * 
 * 
 *
 ***************************************************************************/
#include "cpa.h"
#include <openssl/aes.h>
#include <openssl/ssl.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <string.h>

/* *************************************************************
 *
 * On core crypto for SSL decrypt
 *
 * ************************************************************* */

CpaStatus sampleCodeAesCbcDecrypt(Cpa8U *pKey,
                                  Cpa32U keyLen,
                                  Cpa8U *pIv,
                                  Cpa8U *pIn,
                                  Cpa8U *pOut)
{

#if (OPENSSL_VERSION_NUMBER >= 0x30000000L)
    int len;
    if ((!pIn) || (!pIv) || (!pKey))
        return CPA_STATUS_FAIL;

    EVP_CIPHER_CTX *ctx;

    /* Create and initialise the context */
    if (!(ctx = EVP_CIPHER_CTX_new()))
    {
        ERR_print_errors_fp(stderr);
        return CPA_STATUS_FAIL;
    }

    /* Set algorithm for decryption */
    if (!EVP_DecryptInit_ex(ctx, EVP_aes_256_cbc(), NULL, NULL, NULL))
    {
        ERR_print_errors_fp(stderr);
        goto exit;
    }

    /* Setting Initialization Vector length */
    if (!EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, 16, NULL))
    {
        ERR_print_errors_fp(stderr);
        goto exit;
    }

    /* Initializing key and IV */
    if (!EVP_DecryptInit_ex(ctx, NULL, NULL, pKey, pIv))
    {
        ERR_print_errors_fp(stderr);
        goto exit;
    }

    /*no padding */
    if (!EVP_CIPHER_CTX_set_padding(ctx, 0))
    {
        ERR_print_errors_fp(stderr);
        goto exit;
    }

    /* Decrypt the message to the output buffer */
    if (!EVP_DecryptUpdate(ctx, pOut, &len, pIn, 16))
    {
        ERR_print_errors_fp(stderr);
        goto exit;
    }

    /* Free the cipher context */
    EVP_CIPHER_CTX_free(ctx);
    return CPA_STATUS_SUCCESS;
exit:
    EVP_CIPHER_CTX_free(ctx);
    return CPA_STATUS_FAIL;
#else
    AES_KEY dec_key;
    int i = 0;
    int status = AES_set_decrypt_key(pKey, keyLen << 3, &dec_key);
    if (status == -1)
    {
        return CPA_STATUS_FAIL;
    }
    AES_decrypt(pIn, pOut, &dec_key);

    /* Xor with IV */
    for (i = 0; i < 16; i++)
    {
        pOut[i] = pOut[i] ^ pIv[i];
    }
    return CPA_STATUS_SUCCESS;
#endif
}
