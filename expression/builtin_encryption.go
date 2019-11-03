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

package expression

import (
	"bytes"
	"compress/zlib"
	"crypto/aes"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/encrypt"
)

var (
	_ functionClass = &aesDecryptFunctionClass{}
	_ functionClass = &aesEncryptFunctionClass{}
	_ functionClass = &compressFunctionClass{}
	_ functionClass = &decodeFunctionClass{}
	_ functionClass = &desDecryptFunctionClass{}
	_ functionClass = &desEncryptFunctionClass{}
	_ functionClass = &encodeFunctionClass{}
	_ functionClass = &encryptFunctionClass{}
	_ functionClass = &md5FunctionClass{}
	_ functionClass = &oldPasswordFunctionClass{}
	_ functionClass = &passwordFunctionClass{}
	_ functionClass = &randomBytesFunctionClass{}
	_ functionClass = &sha1FunctionClass{}
	_ functionClass = &sha2FunctionClass{}
	_ functionClass = &uncompressFunctionClass{}
	_ functionClass = &uncompressedLengthFunctionClass{}
	_ functionClass = &validatePasswordStrengthFunctionClass{}
)

var (
	_ builtinFunc = &builtinAesDecryptSig{}
	_ builtinFunc = &builtinAesDecryptIVSig{}
	_ builtinFunc = &builtinAesEncryptSig{}
	_ builtinFunc = &builtinAesEncryptIVSig{}
	_ builtinFunc = &builtinCompressSig{}
	_ builtinFunc = &builtinMD5Sig{}
	_ builtinFunc = &builtinPasswordSig{}
	_ builtinFunc = &builtinRandomBytesSig{}
	_ builtinFunc = &builtinSHA1Sig{}
	_ builtinFunc = &builtinSHA2Sig{}
	_ builtinFunc = &builtinUncompressSig{}
	_ builtinFunc = &builtinUncompressedLengthSig{}
)

// ivSize indicates the initialization vector supplied to aes_decrypt
const ivSize = aes.BlockSize

// aesModeAttr indicates that the key length and iv attribute for specific block_encryption_mode.
// keySize is the key length in bits and mode is the encryption mode.
// ivRequired indicates that initialization vector is required or not.
type aesModeAttr struct {
	modeName   string
	keySize    int
	ivRequired bool
}

var aesModes = map[string]*aesModeAttr{
	//TODO support more modes, permitted mode values are: ECB, CBC, CFB1, CFB8, CFB128, OFB
	"aes-128-ecb": {"ecb", 16, false},
	"aes-192-ecb": {"ecb", 24, false},
	"aes-256-ecb": {"ecb", 32, false},
	"aes-128-cbc": {"cbc", 16, true},
	"aes-192-cbc": {"cbc", 24, true},
	"aes-256-cbc": {"cbc", 32, true},
	"aes-128-ofb": {"ofb", 16, true},
	"aes-192-ofb": {"ofb", 24, true},
	"aes-256-ofb": {"ofb", 32, true},
	"aes-128-cfb": {"cfb", 16, true},
	"aes-192-cfb": {"cfb", 24, true},
	"aes-256-cfb": {"cfb", 32, true},
}

type aesDecryptFunctionClass struct {
	baseFunctionClass
}

func (c *aesDecryptFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_encryption_00000, 0)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 5)
		return nil, c.verifyArgs(args)
	}
	trace_util_0.Count(_builtin_encryption_00000, 1)
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		trace_util_0.Count(_builtin_encryption_00000, 6)
		argTps = append(argTps, types.ETString)
	}
	trace_util_0.Count(_builtin_encryption_00000, 2)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, argTps...)
	bf.tp.Flen = args[0].GetType().Flen // At most.
	types.SetBinChsClnFlag(bf.tp)

	blockMode, _ := ctx.GetSessionVars().GetSystemVar(variable.BlockEncryptionMode)
	mode, exists := aesModes[strings.ToLower(blockMode)]
	if !exists {
		trace_util_0.Count(_builtin_encryption_00000, 7)
		return nil, errors.Errorf("unsupported block encryption mode - %v", blockMode)
	}
	trace_util_0.Count(_builtin_encryption_00000, 3)
	if mode.ivRequired {
		trace_util_0.Count(_builtin_encryption_00000, 8)
		if len(args) != 3 {
			trace_util_0.Count(_builtin_encryption_00000, 10)
			return nil, ErrIncorrectParameterCount.GenWithStackByArgs("aes_decrypt")
		}
		trace_util_0.Count(_builtin_encryption_00000, 9)
		return &builtinAesDecryptIVSig{bf, mode}, nil
	}
	trace_util_0.Count(_builtin_encryption_00000, 4)
	return &builtinAesDecryptSig{bf, mode}, nil
}

type builtinAesDecryptSig struct {
	baseBuiltinFunc
	*aesModeAttr
}

func (b *builtinAesDecryptSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_encryption_00000, 11)
	newSig := &builtinAesDecryptSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.aesModeAttr = b.aesModeAttr
	return newSig
}

// evalString evals AES_DECRYPT(crypt_str, key_key).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
func (b *builtinAesDecryptSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_encryption_00000, 12)
	// According to doc: If either function argument is NULL, the function returns NULL.
	cryptStr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 18)
		return "", true, err
	}
	trace_util_0.Count(_builtin_encryption_00000, 13)
	keyStr, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 19)
		return "", true, err
	}
	trace_util_0.Count(_builtin_encryption_00000, 14)
	if !b.ivRequired && len(b.args) == 3 {
		trace_util_0.Count(_builtin_encryption_00000, 20)
		// For modes that do not require init_vector, it is ignored and a warning is generated if it is specified.
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnOptionIgnored.GenWithStackByArgs("IV"))
	}

	trace_util_0.Count(_builtin_encryption_00000, 15)
	key := encrypt.DeriveKeyMySQL([]byte(keyStr), b.keySize)
	var plainText []byte
	switch b.modeName {
	case "ecb":
		trace_util_0.Count(_builtin_encryption_00000, 21)
		plainText, err = encrypt.AESDecryptWithECB([]byte(cryptStr), key)
	default:
		trace_util_0.Count(_builtin_encryption_00000, 22)
		return "", true, errors.Errorf("unsupported block encryption mode - %v", b.modeName)
	}
	trace_util_0.Count(_builtin_encryption_00000, 16)
	if err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 23)
		return "", true, nil
	}
	trace_util_0.Count(_builtin_encryption_00000, 17)
	return string(plainText), false, nil
}

type builtinAesDecryptIVSig struct {
	baseBuiltinFunc
	*aesModeAttr
}

func (b *builtinAesDecryptIVSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_encryption_00000, 24)
	newSig := &builtinAesDecryptIVSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.aesModeAttr = b.aesModeAttr
	return newSig
}

// evalString evals AES_DECRYPT(crypt_str, key_key, iv).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
func (b *builtinAesDecryptIVSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_encryption_00000, 25)
	// According to doc: If either function argument is NULL, the function returns NULL.
	cryptStr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 32)
		return "", true, err
	}

	trace_util_0.Count(_builtin_encryption_00000, 26)
	keyStr, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 33)
		return "", true, err
	}

	trace_util_0.Count(_builtin_encryption_00000, 27)
	iv, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 34)
		return "", true, err
	}
	trace_util_0.Count(_builtin_encryption_00000, 28)
	if len(iv) < aes.BlockSize {
		trace_util_0.Count(_builtin_encryption_00000, 35)
		return "", true, errIncorrectArgs.GenWithStack("The initialization vector supplied to aes_decrypt is too short. Must be at least %d bytes long", aes.BlockSize)
	}
	// init_vector must be 16 bytes or longer (bytes in excess of 16 are ignored)
	trace_util_0.Count(_builtin_encryption_00000, 29)
	iv = iv[0:aes.BlockSize]

	key := encrypt.DeriveKeyMySQL([]byte(keyStr), b.keySize)
	var plainText []byte
	switch b.modeName {
	case "cbc":
		trace_util_0.Count(_builtin_encryption_00000, 36)
		plainText, err = encrypt.AESDecryptWithCBC([]byte(cryptStr), key, []byte(iv))
	case "ofb":
		trace_util_0.Count(_builtin_encryption_00000, 37)
		plainText, err = encrypt.AESDecryptWithOFB([]byte(cryptStr), key, []byte(iv))
	case "cfb":
		trace_util_0.Count(_builtin_encryption_00000, 38)
		plainText, err = encrypt.AESDecryptWithCFB([]byte(cryptStr), key, []byte(iv))
	default:
		trace_util_0.Count(_builtin_encryption_00000, 39)
		return "", true, errors.Errorf("unsupported block encryption mode - %v", b.modeName)
	}
	trace_util_0.Count(_builtin_encryption_00000, 30)
	if err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 40)
		return "", true, nil
	}
	trace_util_0.Count(_builtin_encryption_00000, 31)
	return string(plainText), false, nil
}

type aesEncryptFunctionClass struct {
	baseFunctionClass
}

func (c *aesEncryptFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_encryption_00000, 41)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 46)
		return nil, c.verifyArgs(args)
	}
	trace_util_0.Count(_builtin_encryption_00000, 42)
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		trace_util_0.Count(_builtin_encryption_00000, 47)
		argTps = append(argTps, types.ETString)
	}
	trace_util_0.Count(_builtin_encryption_00000, 43)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, argTps...)
	bf.tp.Flen = aes.BlockSize * (args[0].GetType().Flen/aes.BlockSize + 1) // At most.
	types.SetBinChsClnFlag(bf.tp)

	blockMode, _ := ctx.GetSessionVars().GetSystemVar(variable.BlockEncryptionMode)
	mode, exists := aesModes[strings.ToLower(blockMode)]
	if !exists {
		trace_util_0.Count(_builtin_encryption_00000, 48)
		return nil, errors.Errorf("unsupported block encryption mode - %v", blockMode)
	}
	trace_util_0.Count(_builtin_encryption_00000, 44)
	if mode.ivRequired {
		trace_util_0.Count(_builtin_encryption_00000, 49)
		if len(args) != 3 {
			trace_util_0.Count(_builtin_encryption_00000, 51)
			return nil, ErrIncorrectParameterCount.GenWithStackByArgs("aes_encrypt")
		}
		trace_util_0.Count(_builtin_encryption_00000, 50)
		return &builtinAesEncryptIVSig{bf, mode}, nil
	}
	trace_util_0.Count(_builtin_encryption_00000, 45)
	return &builtinAesEncryptSig{bf, mode}, nil
}

type builtinAesEncryptSig struct {
	baseBuiltinFunc
	*aesModeAttr
}

func (b *builtinAesEncryptSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_encryption_00000, 52)
	newSig := &builtinAesEncryptSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.aesModeAttr = b.aesModeAttr
	return newSig
}

// evalString evals AES_ENCRYPT(str, key_str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
func (b *builtinAesEncryptSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_encryption_00000, 53)
	// According to doc: If either function argument is NULL, the function returns NULL.
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 59)
		return "", true, err
	}
	trace_util_0.Count(_builtin_encryption_00000, 54)
	keyStr, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 60)
		return "", true, err
	}
	trace_util_0.Count(_builtin_encryption_00000, 55)
	if !b.ivRequired && len(b.args) == 3 {
		trace_util_0.Count(_builtin_encryption_00000, 61)
		// For modes that do not require init_vector, it is ignored and a warning is generated if it is specified.
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnOptionIgnored.GenWithStackByArgs("IV"))
	}

	trace_util_0.Count(_builtin_encryption_00000, 56)
	key := encrypt.DeriveKeyMySQL([]byte(keyStr), b.keySize)
	var cipherText []byte
	switch b.modeName {
	case "ecb":
		trace_util_0.Count(_builtin_encryption_00000, 62)
		cipherText, err = encrypt.AESEncryptWithECB([]byte(str), key)
	default:
		trace_util_0.Count(_builtin_encryption_00000, 63)
		return "", true, errors.Errorf("unsupported block encryption mode - %v", b.modeName)
	}
	trace_util_0.Count(_builtin_encryption_00000, 57)
	if err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 64)
		return "", true, nil
	}
	trace_util_0.Count(_builtin_encryption_00000, 58)
	return string(cipherText), false, nil
}

type builtinAesEncryptIVSig struct {
	baseBuiltinFunc
	*aesModeAttr
}

func (b *builtinAesEncryptIVSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_encryption_00000, 65)
	newSig := &builtinAesEncryptIVSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.aesModeAttr = b.aesModeAttr
	return newSig
}

// evalString evals AES_ENCRYPT(str, key_str, iv).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
func (b *builtinAesEncryptIVSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_encryption_00000, 66)
	// According to doc: If either function argument is NULL, the function returns NULL.
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 73)
		return "", true, err
	}

	trace_util_0.Count(_builtin_encryption_00000, 67)
	keyStr, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 74)
		return "", true, err
	}

	trace_util_0.Count(_builtin_encryption_00000, 68)
	iv, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 75)
		return "", true, err
	}
	trace_util_0.Count(_builtin_encryption_00000, 69)
	if len(iv) < aes.BlockSize {
		trace_util_0.Count(_builtin_encryption_00000, 76)
		return "", true, errIncorrectArgs.GenWithStack("The initialization vector supplied to aes_encrypt is too short. Must be at least %d bytes long", aes.BlockSize)
	}
	// init_vector must be 16 bytes or longer (bytes in excess of 16 are ignored)
	trace_util_0.Count(_builtin_encryption_00000, 70)
	iv = iv[0:aes.BlockSize]

	key := encrypt.DeriveKeyMySQL([]byte(keyStr), b.keySize)
	var cipherText []byte
	switch b.modeName {
	case "cbc":
		trace_util_0.Count(_builtin_encryption_00000, 77)
		cipherText, err = encrypt.AESEncryptWithCBC([]byte(str), key, []byte(iv))
	case "ofb":
		trace_util_0.Count(_builtin_encryption_00000, 78)
		cipherText, err = encrypt.AESEncryptWithOFB([]byte(str), key, []byte(iv))
	case "cfb":
		trace_util_0.Count(_builtin_encryption_00000, 79)
		cipherText, err = encrypt.AESEncryptWithCFB([]byte(str), key, []byte(iv))
	default:
		trace_util_0.Count(_builtin_encryption_00000, 80)
		return "", true, errors.Errorf("unsupported block encryption mode - %v", b.modeName)
	}
	trace_util_0.Count(_builtin_encryption_00000, 71)
	if err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 81)
		return "", true, nil
	}
	trace_util_0.Count(_builtin_encryption_00000, 72)
	return string(cipherText), false, nil
}

type decodeFunctionClass struct {
	baseFunctionClass
}

func (c *decodeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_encryption_00000, 82)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 84)
		return nil, err
	}

	trace_util_0.Count(_builtin_encryption_00000, 83)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETString)

	bf.tp.Flen = args[0].GetType().Flen
	sig := &builtinDecodeSig{bf}
	return sig, nil
}

type builtinDecodeSig struct {
	baseBuiltinFunc
}

func (b *builtinDecodeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_encryption_00000, 85)
	newSig := &builtinDecodeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals DECODE(str, password_str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_decode
func (b *builtinDecodeSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_encryption_00000, 86)
	dataStr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 89)
		return "", true, err
	}

	trace_util_0.Count(_builtin_encryption_00000, 87)
	passwordStr, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 90)
		return "", true, err
	}

	trace_util_0.Count(_builtin_encryption_00000, 88)
	decodeStr, err := encrypt.SQLDecode(dataStr, passwordStr)
	return decodeStr, false, err
}

type desDecryptFunctionClass struct {
	baseFunctionClass
}

func (c *desDecryptFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_encryption_00000, 91)
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "DES_DECRYPT")
}

type desEncryptFunctionClass struct {
	baseFunctionClass
}

func (c *desEncryptFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_encryption_00000, 92)
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "DES_ENCRYPT")
}

type encodeFunctionClass struct {
	baseFunctionClass
}

func (c *encodeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_encryption_00000, 93)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 95)
		return nil, err
	}

	trace_util_0.Count(_builtin_encryption_00000, 94)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETString)

	bf.tp.Flen = args[0].GetType().Flen
	sig := &builtinEncodeSig{bf}
	return sig, nil
}

type builtinEncodeSig struct {
	baseBuiltinFunc
}

func (b *builtinEncodeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_encryption_00000, 96)
	newSig := &builtinEncodeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals ENCODE(crypt_str, password_str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_encode
func (b *builtinEncodeSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_encryption_00000, 97)
	decodeStr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 100)
		return "", true, err
	}

	trace_util_0.Count(_builtin_encryption_00000, 98)
	passwordStr, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 101)
		return "", true, err
	}

	trace_util_0.Count(_builtin_encryption_00000, 99)
	dataStr, err := encrypt.SQLEncode(decodeStr, passwordStr)
	return dataStr, false, err
}

type encryptFunctionClass struct {
	baseFunctionClass
}

func (c *encryptFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_encryption_00000, 102)
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "ENCRYPT")
}

type oldPasswordFunctionClass struct {
	baseFunctionClass
}

func (c *oldPasswordFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_encryption_00000, 103)
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "OLD_PASSWORD")
}

type passwordFunctionClass struct {
	baseFunctionClass
}

func (c *passwordFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_encryption_00000, 104)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 106)
		return nil, err
	}
	trace_util_0.Count(_builtin_encryption_00000, 105)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	bf.tp.Flen = mysql.PWDHashLen + 1
	sig := &builtinPasswordSig{bf}
	return sig, nil
}

type builtinPasswordSig struct {
	baseBuiltinFunc
}

func (b *builtinPasswordSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_encryption_00000, 107)
	newSig := &builtinPasswordSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinPasswordSig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_password
func (b *builtinPasswordSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	trace_util_0.Count(_builtin_encryption_00000, 108)
	pass, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 111)
		return "", err != nil, err
	}

	trace_util_0.Count(_builtin_encryption_00000, 109)
	if len(pass) == 0 {
		trace_util_0.Count(_builtin_encryption_00000, 112)
		return "", false, nil
	}

	// We should append a warning here because function "PASSWORD" is deprecated since MySQL 5.7.6.
	// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_password
	trace_util_0.Count(_builtin_encryption_00000, 110)
	b.ctx.GetSessionVars().StmtCtx.AppendWarning(errDeprecatedSyntaxNoReplacement.GenWithStackByArgs("PASSWORD"))

	return auth.EncodePassword(pass), false, nil
}

type randomBytesFunctionClass struct {
	baseFunctionClass
}

func (c *randomBytesFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_encryption_00000, 113)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 115)
		return nil, err
	}
	trace_util_0.Count(_builtin_encryption_00000, 114)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETInt)
	bf.tp.Flen = 1024 // Max allowed random bytes
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinRandomBytesSig{bf}
	return sig, nil
}

type builtinRandomBytesSig struct {
	baseBuiltinFunc
}

func (b *builtinRandomBytesSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_encryption_00000, 116)
	newSig := &builtinRandomBytesSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals RANDOM_BYTES(len).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_random-bytes
func (b *builtinRandomBytesSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_encryption_00000, 117)
	len, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 121)
		return "", true, err
	}
	trace_util_0.Count(_builtin_encryption_00000, 118)
	if len < 1 || len > 1024 {
		trace_util_0.Count(_builtin_encryption_00000, 122)
		return "", false, types.ErrOverflow.GenWithStackByArgs("length", "random_bytes")
	}
	trace_util_0.Count(_builtin_encryption_00000, 119)
	buf := make([]byte, len)
	if n, err := rand.Read(buf); err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 123)
		return "", true, err
	} else {
		trace_util_0.Count(_builtin_encryption_00000, 124)
		if int64(n) != len {
			trace_util_0.Count(_builtin_encryption_00000, 125)
			return "", false, errors.New("fail to generate random bytes")
		}
	}
	trace_util_0.Count(_builtin_encryption_00000, 120)
	return string(buf), false, nil
}

type md5FunctionClass struct {
	baseFunctionClass
}

func (c *md5FunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_encryption_00000, 126)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 128)
		return nil, err
	}
	trace_util_0.Count(_builtin_encryption_00000, 127)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	bf.tp.Flen = 32
	sig := &builtinMD5Sig{bf}
	return sig, nil
}

type builtinMD5Sig struct {
	baseBuiltinFunc
}

func (b *builtinMD5Sig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_encryption_00000, 129)
	newSig := &builtinMD5Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinMD5Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_md5
func (b *builtinMD5Sig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_encryption_00000, 130)
	arg, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 132)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_encryption_00000, 131)
	sum := md5.Sum([]byte(arg))
	hexStr := fmt.Sprintf("%x", sum)
	return hexStr, false, nil
}

type sha1FunctionClass struct {
	baseFunctionClass
}

func (c *sha1FunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_encryption_00000, 133)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 135)
		return nil, err
	}
	trace_util_0.Count(_builtin_encryption_00000, 134)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	bf.tp.Flen = 40
	sig := &builtinSHA1Sig{bf}
	return sig, nil
}

type builtinSHA1Sig struct {
	baseBuiltinFunc
}

func (b *builtinSHA1Sig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_encryption_00000, 136)
	newSig := &builtinSHA1Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals SHA1(str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_sha1
// The value is returned as a string of 40 hexadecimal digits, or NULL if the argument was NULL.
func (b *builtinSHA1Sig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_encryption_00000, 137)
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 140)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_encryption_00000, 138)
	hasher := sha1.New()
	_, err = hasher.Write([]byte(str))
	if err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 141)
		return "", true, err
	}
	trace_util_0.Count(_builtin_encryption_00000, 139)
	return fmt.Sprintf("%x", hasher.Sum(nil)), false, nil
}

type sha2FunctionClass struct {
	baseFunctionClass
}

func (c *sha2FunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_encryption_00000, 142)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 144)
		return nil, err
	}
	trace_util_0.Count(_builtin_encryption_00000, 143)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETInt)
	bf.tp.Flen = 128 // sha512
	sig := &builtinSHA2Sig{bf}
	return sig, nil
}

type builtinSHA2Sig struct {
	baseBuiltinFunc
}

func (b *builtinSHA2Sig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_encryption_00000, 145)
	newSig := &builtinSHA2Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// Supported hash length of SHA-2 family
const (
	SHA0   = 0
	SHA224 = 224
	SHA256 = 256
	SHA384 = 384
	SHA512 = 512
)

// evalString evals SHA2(str, hash_length).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_sha2
func (b *builtinSHA2Sig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_encryption_00000, 146)
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 152)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_encryption_00000, 147)
	hashLength, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 153)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_encryption_00000, 148)
	var hasher hash.Hash
	switch int(hashLength) {
	case SHA0, SHA256:
		trace_util_0.Count(_builtin_encryption_00000, 154)
		hasher = sha256.New()
	case SHA224:
		trace_util_0.Count(_builtin_encryption_00000, 155)
		hasher = sha256.New224()
	case SHA384:
		trace_util_0.Count(_builtin_encryption_00000, 156)
		hasher = sha512.New384()
	case SHA512:
		trace_util_0.Count(_builtin_encryption_00000, 157)
		hasher = sha512.New()
	}
	trace_util_0.Count(_builtin_encryption_00000, 149)
	if hasher == nil {
		trace_util_0.Count(_builtin_encryption_00000, 158)
		return "", true, nil
	}

	trace_util_0.Count(_builtin_encryption_00000, 150)
	_, err = hasher.Write([]byte(str))
	if err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 159)
		return "", true, err
	}
	trace_util_0.Count(_builtin_encryption_00000, 151)
	return fmt.Sprintf("%x", hasher.Sum(nil)), false, nil
}

// deflate compresses a string using the DEFLATE format.
func deflate(data []byte) ([]byte, error) {
	trace_util_0.Count(_builtin_encryption_00000, 160)
	var buffer bytes.Buffer
	w := zlib.NewWriter(&buffer)
	if _, err := w.Write(data); err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 163)
		return nil, err
	}
	trace_util_0.Count(_builtin_encryption_00000, 161)
	if err := w.Close(); err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 164)
		return nil, err
	}
	trace_util_0.Count(_builtin_encryption_00000, 162)
	return buffer.Bytes(), nil
}

// inflate uncompresses a string using the DEFLATE format.
func inflate(compressStr []byte) ([]byte, error) {
	trace_util_0.Count(_builtin_encryption_00000, 165)
	reader := bytes.NewReader(compressStr)
	var out bytes.Buffer
	r, err := zlib.NewReader(reader)
	if err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 168)
		return nil, err
	}
	trace_util_0.Count(_builtin_encryption_00000, 166)
	if _, err = io.Copy(&out, r); err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 169)
		return nil, err
	}
	trace_util_0.Count(_builtin_encryption_00000, 167)
	err = r.Close()
	return out.Bytes(), err
}

type compressFunctionClass struct {
	baseFunctionClass
}

func (c *compressFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_encryption_00000, 170)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 173)
		return nil, err
	}
	trace_util_0.Count(_builtin_encryption_00000, 171)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	srcLen := args[0].GetType().Flen
	compressBound := srcLen + (srcLen >> 12) + (srcLen >> 14) + (srcLen >> 25) + 13
	if compressBound > mysql.MaxBlobWidth {
		trace_util_0.Count(_builtin_encryption_00000, 174)
		compressBound = mysql.MaxBlobWidth
	}
	trace_util_0.Count(_builtin_encryption_00000, 172)
	bf.tp.Flen = compressBound
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinCompressSig{bf}
	return sig, nil
}

type builtinCompressSig struct {
	baseBuiltinFunc
}

func (b *builtinCompressSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_encryption_00000, 175)
	newSig := &builtinCompressSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals COMPRESS(str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_compress
func (b *builtinCompressSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_encryption_00000, 176)
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 182)
		return "", true, err
	}

	// According to doc: Empty strings are stored as empty strings.
	trace_util_0.Count(_builtin_encryption_00000, 177)
	if len(str) == 0 {
		trace_util_0.Count(_builtin_encryption_00000, 183)
		return "", false, nil
	}

	trace_util_0.Count(_builtin_encryption_00000, 178)
	compressed, err := deflate([]byte(str))
	if err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 184)
		return "", true, nil
	}

	trace_util_0.Count(_builtin_encryption_00000, 179)
	resultLength := 4 + len(compressed)

	// append "." if ends with space
	shouldAppendSuffix := compressed[len(compressed)-1] == 32
	if shouldAppendSuffix {
		trace_util_0.Count(_builtin_encryption_00000, 185)
		resultLength++
	}

	trace_util_0.Count(_builtin_encryption_00000, 180)
	buffer := make([]byte, resultLength)
	binary.LittleEndian.PutUint32(buffer, uint32(len(str)))
	copy(buffer[4:], compressed)

	if shouldAppendSuffix {
		trace_util_0.Count(_builtin_encryption_00000, 186)
		buffer[len(buffer)-1] = '.'
	}

	trace_util_0.Count(_builtin_encryption_00000, 181)
	return string(buffer), false, nil
}

type uncompressFunctionClass struct {
	baseFunctionClass
}

func (c *uncompressFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_encryption_00000, 187)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 189)
		return nil, err
	}
	trace_util_0.Count(_builtin_encryption_00000, 188)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	bf.tp.Flen = mysql.MaxBlobWidth
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinUncompressSig{bf}
	return sig, nil
}

type builtinUncompressSig struct {
	baseBuiltinFunc
}

func (b *builtinUncompressSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_encryption_00000, 190)
	newSig := &builtinUncompressSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals UNCOMPRESS(compressed_string).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_uncompress
func (b *builtinUncompressSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_encryption_00000, 191)
	sc := b.ctx.GetSessionVars().StmtCtx
	payload, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 197)
		return "", true, err
	}
	trace_util_0.Count(_builtin_encryption_00000, 192)
	if len(payload) == 0 {
		trace_util_0.Count(_builtin_encryption_00000, 198)
		return "", false, nil
	}
	trace_util_0.Count(_builtin_encryption_00000, 193)
	if len(payload) <= 4 {
		trace_util_0.Count(_builtin_encryption_00000, 199)
		// corrupted
		sc.AppendWarning(errZlibZData)
		return "", true, nil
	}
	trace_util_0.Count(_builtin_encryption_00000, 194)
	length := binary.LittleEndian.Uint32([]byte(payload[0:4]))
	bytes, err := inflate([]byte(payload[4:]))
	if err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 200)
		sc.AppendWarning(errZlibZData)
		return "", true, nil
	}
	trace_util_0.Count(_builtin_encryption_00000, 195)
	if length < uint32(len(bytes)) {
		trace_util_0.Count(_builtin_encryption_00000, 201)
		sc.AppendWarning(errZlibZBuf)
		return "", true, nil
	}
	trace_util_0.Count(_builtin_encryption_00000, 196)
	return string(bytes), false, nil
}

type uncompressedLengthFunctionClass struct {
	baseFunctionClass
}

func (c *uncompressedLengthFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_encryption_00000, 202)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 204)
		return nil, err
	}
	trace_util_0.Count(_builtin_encryption_00000, 203)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
	bf.tp.Flen = 10
	sig := &builtinUncompressedLengthSig{bf}
	return sig, nil
}

type builtinUncompressedLengthSig struct {
	baseBuiltinFunc
}

func (b *builtinUncompressedLengthSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_encryption_00000, 205)
	newSig := &builtinUncompressedLengthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals UNCOMPRESSED_LENGTH(str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_uncompressed-length
func (b *builtinUncompressedLengthSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_encryption_00000, 206)
	sc := b.ctx.GetSessionVars().StmtCtx
	payload, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_encryption_00000, 210)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_encryption_00000, 207)
	if len(payload) == 0 {
		trace_util_0.Count(_builtin_encryption_00000, 211)
		return 0, false, nil
	}
	trace_util_0.Count(_builtin_encryption_00000, 208)
	if len(payload) <= 4 {
		trace_util_0.Count(_builtin_encryption_00000, 212)
		// corrupted
		sc.AppendWarning(errZlibZData)
		return 0, false, nil
	}
	trace_util_0.Count(_builtin_encryption_00000, 209)
	len := binary.LittleEndian.Uint32([]byte(payload)[0:4])
	return int64(len), false, nil
}

type validatePasswordStrengthFunctionClass struct {
	baseFunctionClass
}

func (c *validatePasswordStrengthFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_encryption_00000, 213)
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "VALIDATE_PASSWORD_STRENGTH")
}

var _builtin_encryption_00000 = "expression/builtin_encryption.go"
