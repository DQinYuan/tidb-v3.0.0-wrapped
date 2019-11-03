// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package encrypt

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/trace_util_0"
)

type ecb struct {
	b         cipher.Block
	blockSize int
}

func newECB(b cipher.Block) *ecb {
	trace_util_0.Count(_aes_00000, 0)
	return &ecb{
		b:         b,
		blockSize: b.BlockSize(),
	}
}

type ecbEncrypter ecb

// BlockSize implements BlockMode.BlockSize interface.
func (x *ecbEncrypter) BlockSize() int { trace_util_0.Count(_aes_00000, 1); return x.blockSize }

// CryptBlocks implements BlockMode.CryptBlocks interface.
func (x *ecbEncrypter) CryptBlocks(dst, src []byte) {
	trace_util_0.Count(_aes_00000, 2)
	if len(src)%x.blockSize != 0 {
		trace_util_0.Count(_aes_00000, 6)
		panic("ECBEncrypter: input not full blocks")
	}
	trace_util_0.Count(_aes_00000, 3)
	if len(dst) < len(src) {
		trace_util_0.Count(_aes_00000, 7)
		panic("ECBEncrypter: output smaller than input")
	}
	// See https://en.wikipedia.org/wiki/Block_cipher_mode_of_operation#Electronic_Codebook_.28ECB.29
	trace_util_0.Count(_aes_00000, 4)
	for len(src) > 0 {
		trace_util_0.Count(_aes_00000, 8)
		x.b.Encrypt(dst, src[:x.blockSize])
		src = src[x.blockSize:]
		dst = dst[x.blockSize:]
	}
	trace_util_0.Count(_aes_00000, 5)
	return
}

// newECBEncrypter creates an AES encrypter with ecb mode.
func newECBEncrypter(b cipher.Block) cipher.BlockMode {
	trace_util_0.Count(_aes_00000, 9)
	return (*ecbEncrypter)(newECB(b))
}

type ecbDecrypter ecb

// BlockSize implements BlockMode.BlockSize interface.
func (x *ecbDecrypter) BlockSize() int { trace_util_0.Count(_aes_00000, 10); return x.blockSize }

// CryptBlocks implements BlockMode.CryptBlocks interface.
func (x *ecbDecrypter) CryptBlocks(dst, src []byte) {
	trace_util_0.Count(_aes_00000, 11)
	if len(src)%x.blockSize != 0 {
		trace_util_0.Count(_aes_00000, 14)
		panic("ECBDecrypter: input not full blocks")
	}
	trace_util_0.Count(_aes_00000, 12)
	if len(dst) < len(src) {
		trace_util_0.Count(_aes_00000, 15)
		panic("ECBDecrypter: output smaller than input")
	}
	// See https://en.wikipedia.org/wiki/Block_cipher_mode_of_operation#Electronic_Codebook_.28ECB.29
	trace_util_0.Count(_aes_00000, 13)
	for len(src) > 0 {
		trace_util_0.Count(_aes_00000, 16)
		x.b.Decrypt(dst, src[:x.blockSize])
		src = src[x.blockSize:]
		dst = dst[x.blockSize:]
	}
}

func newECBDecrypter(b cipher.Block) cipher.BlockMode {
	trace_util_0.Count(_aes_00000, 17)
	return (*ecbDecrypter)(newECB(b))
}

// PKCS7Pad pads data using PKCS7.
// See hhttp://tools.ietf.org/html/rfc2315.
func PKCS7Pad(data []byte, blockSize int) ([]byte, error) {
	trace_util_0.Count(_aes_00000, 18)
	length := len(data)
	padLen := blockSize - (length % blockSize)
	padText := bytes.Repeat([]byte{byte(padLen)}, padLen)
	return append(data, padText...), nil
}

// PKCS7Unpad unpads data using PKCS7.
// See http://tools.ietf.org/html/rfc2315.
func PKCS7Unpad(data []byte, blockSize int) ([]byte, error) {
	trace_util_0.Count(_aes_00000, 19)
	length := len(data)
	if length == 0 {
		trace_util_0.Count(_aes_00000, 24)
		return nil, errors.New("Invalid padding size")
	}
	trace_util_0.Count(_aes_00000, 20)
	if length%blockSize != 0 {
		trace_util_0.Count(_aes_00000, 25)
		return nil, errors.New("Invalid padding size")
	}
	trace_util_0.Count(_aes_00000, 21)
	pad := data[length-1]
	padLen := int(pad)
	if padLen > blockSize || padLen == 0 {
		trace_util_0.Count(_aes_00000, 26)
		return nil, errors.New("Invalid padding size")
	}
	// TODO: Fix timing attack here.
	trace_util_0.Count(_aes_00000, 22)
	for _, v := range data[length-padLen : length-1] {
		trace_util_0.Count(_aes_00000, 27)
		if v != pad {
			trace_util_0.Count(_aes_00000, 28)
			return nil, errors.New("Invalid padding")
		}
	}
	trace_util_0.Count(_aes_00000, 23)
	return data[:length-padLen], nil
}

// AESEncryptWithECB encrypts data using AES with ECB mode.
func AESEncryptWithECB(str, key []byte) ([]byte, error) {
	trace_util_0.Count(_aes_00000, 29)
	cb, err := aes.NewCipher(key)
	if err != nil {
		trace_util_0.Count(_aes_00000, 31)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_aes_00000, 30)
	mode := newECBEncrypter(cb)
	return aesEncrypt(str, mode)
}

// AESDecryptWithECB decrypts data using AES with ECB mode.
func AESDecryptWithECB(cryptStr, key []byte) ([]byte, error) {
	trace_util_0.Count(_aes_00000, 32)
	cb, err := aes.NewCipher(key)
	if err != nil {
		trace_util_0.Count(_aes_00000, 34)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_aes_00000, 33)
	mode := newECBDecrypter(cb)
	return aesDecrypt(cryptStr, mode)
}

// DeriveKeyMySQL derives the encryption key from a password in MySQL algorithm.
// See https://security.stackexchange.com/questions/4863/mysql-aes-encrypt-key-length.
func DeriveKeyMySQL(key []byte, blockSize int) []byte {
	trace_util_0.Count(_aes_00000, 35)
	rKey := make([]byte, blockSize)
	rIdx := 0
	for _, k := range key {
		trace_util_0.Count(_aes_00000, 37)
		if rIdx == blockSize {
			trace_util_0.Count(_aes_00000, 39)
			rIdx = 0
		}
		trace_util_0.Count(_aes_00000, 38)
		rKey[rIdx] ^= k
		rIdx++
	}
	trace_util_0.Count(_aes_00000, 36)
	return rKey
}

// AESEncryptWithCBC encrypts data using AES with CBC mode.
func AESEncryptWithCBC(str, key []byte, iv []byte) ([]byte, error) {
	trace_util_0.Count(_aes_00000, 40)
	cb, err := aes.NewCipher(key)
	if err != nil {
		trace_util_0.Count(_aes_00000, 42)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_aes_00000, 41)
	mode := cipher.NewCBCEncrypter(cb, iv)
	return aesEncrypt(str, mode)
}

// AESDecryptWithCBC decrypts data using AES with CBC mode.
func AESDecryptWithCBC(cryptStr, key []byte, iv []byte) ([]byte, error) {
	trace_util_0.Count(_aes_00000, 43)
	cb, err := aes.NewCipher(key)
	if err != nil {
		trace_util_0.Count(_aes_00000, 45)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_aes_00000, 44)
	mode := cipher.NewCBCDecrypter(cb, iv)
	return aesDecrypt(cryptStr, mode)
}

// AESEncryptWithOFB encrypts data using AES with OFB mode.
func AESEncryptWithOFB(plainStr []byte, key []byte, iv []byte) ([]byte, error) {
	trace_util_0.Count(_aes_00000, 46)
	cb, err := aes.NewCipher(key)
	if err != nil {
		trace_util_0.Count(_aes_00000, 48)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_aes_00000, 47)
	mode := cipher.NewOFB(cb, iv)
	crypted := make([]byte, len(plainStr))
	mode.XORKeyStream(crypted, plainStr)
	return crypted, nil
}

// AESDecryptWithOFB decrypts data using AES with OFB mode.
func AESDecryptWithOFB(cipherStr []byte, key []byte, iv []byte) ([]byte, error) {
	trace_util_0.Count(_aes_00000, 49)
	cb, err := aes.NewCipher(key)
	if err != nil {
		trace_util_0.Count(_aes_00000, 51)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_aes_00000, 50)
	mode := cipher.NewOFB(cb, iv)
	plainStr := make([]byte, len(cipherStr))
	mode.XORKeyStream(plainStr, cipherStr)
	return plainStr, nil
}

// AESEncryptWithCFB decrypts data using AES with CFB mode.
func AESEncryptWithCFB(cryptStr, key []byte, iv []byte) ([]byte, error) {
	trace_util_0.Count(_aes_00000, 52)
	cb, err := aes.NewCipher(key)
	if err != nil {
		trace_util_0.Count(_aes_00000, 54)
		return nil, err
	}
	trace_util_0.Count(_aes_00000, 53)
	cfb := cipher.NewCFBEncrypter(cb, iv)
	crypted := make([]byte, len(cryptStr))
	cfb.XORKeyStream(crypted, cryptStr)
	return crypted, nil
}

// AESDecryptWithCFB decrypts data using AES with CFB mode.
func AESDecryptWithCFB(cryptStr, key []byte, iv []byte) ([]byte, error) {
	trace_util_0.Count(_aes_00000, 55)
	cb, err := aes.NewCipher(key)
	if err != nil {
		trace_util_0.Count(_aes_00000, 57)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_aes_00000, 56)
	cfb := cipher.NewCFBDecrypter(cb, []byte(iv))
	dst := make([]byte, len(cryptStr))
	cfb.XORKeyStream(dst, cryptStr)
	return dst, nil
}

// aesDecrypt decrypts data using AES.
func aesDecrypt(cryptStr []byte, mode cipher.BlockMode) ([]byte, error) {
	trace_util_0.Count(_aes_00000, 58)
	blockSize := mode.BlockSize()
	if len(cryptStr)%blockSize != 0 {
		trace_util_0.Count(_aes_00000, 61)
		return nil, errors.New("Corrupted data")
	}
	trace_util_0.Count(_aes_00000, 59)
	data := make([]byte, len(cryptStr))
	mode.CryptBlocks(data, cryptStr)
	plain, err := PKCS7Unpad(data, blockSize)
	if err != nil {
		trace_util_0.Count(_aes_00000, 62)
		return nil, err
	}
	trace_util_0.Count(_aes_00000, 60)
	return plain, nil
}

// aesEncrypt encrypts data using AES.
func aesEncrypt(str []byte, mode cipher.BlockMode) ([]byte, error) {
	trace_util_0.Count(_aes_00000, 63)
	blockSize := mode.BlockSize()
	// The str arguments can be any length, and padding is automatically added to
	// str so it is a multiple of a block as required by block-based algorithms such as AES.
	// This padding is automatically removed by the AES_DECRYPT() function.
	data, err := PKCS7Pad(str, blockSize)
	if err != nil {
		trace_util_0.Count(_aes_00000, 65)
		return nil, err
	}
	trace_util_0.Count(_aes_00000, 64)
	crypted := make([]byte, len(data))
	mode.CryptBlocks(crypted, data)
	return crypted, nil
}

var _aes_00000 = "util/encrypt/aes.go"
