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
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tipb/go-tipb"
)

func pbTypeToFieldType(tp *tipb.FieldType) *types.FieldType {
	trace_util_0.Count(_distsql_builtin_00000, 0)
	return &types.FieldType{
		Tp:      byte(tp.Tp),
		Flag:    uint(tp.Flag),
		Flen:    int(tp.Flen),
		Decimal: int(tp.Decimal),
		Charset: tp.Charset,
		Collate: mysql.Collations[uint8(tp.Collate)],
	}
}

func getSignatureByPB(ctx sessionctx.Context, sigCode tipb.ScalarFuncSig, tp *tipb.FieldType, args []Expression) (f builtinFunc, e error) {
	trace_util_0.Count(_distsql_builtin_00000, 1)
	fieldTp := pbTypeToFieldType(tp)
	base := newBaseBuiltinFunc(ctx, args)
	base.tp = fieldTp
	switch sigCode {
	case tipb.ScalarFuncSig_CastIntAsInt:
		trace_util_0.Count(_distsql_builtin_00000, 3)
		f = &builtinCastIntAsIntSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastRealAsInt:
		trace_util_0.Count(_distsql_builtin_00000, 4)
		f = &builtinCastRealAsIntSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastDecimalAsInt:
		trace_util_0.Count(_distsql_builtin_00000, 5)
		f = &builtinCastDecimalAsIntSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastDurationAsInt:
		trace_util_0.Count(_distsql_builtin_00000, 6)
		f = &builtinCastDurationAsIntSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastTimeAsInt:
		trace_util_0.Count(_distsql_builtin_00000, 7)
		f = &builtinCastTimeAsIntSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastStringAsInt:
		trace_util_0.Count(_distsql_builtin_00000, 8)
		f = &builtinCastStringAsIntSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastJsonAsInt:
		trace_util_0.Count(_distsql_builtin_00000, 9)
		f = &builtinCastJSONAsIntSig{newBaseBuiltinCastFunc(base, false)}

	case tipb.ScalarFuncSig_CastIntAsReal:
		trace_util_0.Count(_distsql_builtin_00000, 10)
		f = &builtinCastIntAsRealSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastRealAsReal:
		trace_util_0.Count(_distsql_builtin_00000, 11)
		f = &builtinCastRealAsRealSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastDecimalAsReal:
		trace_util_0.Count(_distsql_builtin_00000, 12)
		f = &builtinCastDecimalAsRealSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastDurationAsReal:
		trace_util_0.Count(_distsql_builtin_00000, 13)
		f = &builtinCastDurationAsRealSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastTimeAsReal:
		trace_util_0.Count(_distsql_builtin_00000, 14)
		f = &builtinCastTimeAsRealSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastStringAsReal:
		trace_util_0.Count(_distsql_builtin_00000, 15)
		f = &builtinCastStringAsRealSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastJsonAsReal:
		trace_util_0.Count(_distsql_builtin_00000, 16)
		f = &builtinCastJSONAsRealSig{newBaseBuiltinCastFunc(base, false)}

	case tipb.ScalarFuncSig_CastIntAsDecimal:
		trace_util_0.Count(_distsql_builtin_00000, 17)
		f = &builtinCastIntAsDecimalSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastRealAsDecimal:
		trace_util_0.Count(_distsql_builtin_00000, 18)
		f = &builtinCastRealAsDecimalSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastDecimalAsDecimal:
		trace_util_0.Count(_distsql_builtin_00000, 19)
		f = &builtinCastDecimalAsDecimalSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastDurationAsDecimal:
		trace_util_0.Count(_distsql_builtin_00000, 20)
		f = &builtinCastDurationAsDecimalSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastTimeAsDecimal:
		trace_util_0.Count(_distsql_builtin_00000, 21)
		f = &builtinCastTimeAsDecimalSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastStringAsDecimal:
		trace_util_0.Count(_distsql_builtin_00000, 22)
		f = &builtinCastStringAsDecimalSig{newBaseBuiltinCastFunc(base, false)}
	case tipb.ScalarFuncSig_CastJsonAsDecimal:
		trace_util_0.Count(_distsql_builtin_00000, 23)
		f = &builtinCastJSONAsDecimalSig{newBaseBuiltinCastFunc(base, false)}

	case tipb.ScalarFuncSig_CastIntAsTime:
		trace_util_0.Count(_distsql_builtin_00000, 24)
		f = &builtinCastIntAsTimeSig{base}
	case tipb.ScalarFuncSig_CastRealAsTime:
		trace_util_0.Count(_distsql_builtin_00000, 25)
		f = &builtinCastRealAsTimeSig{base}
	case tipb.ScalarFuncSig_CastDecimalAsTime:
		trace_util_0.Count(_distsql_builtin_00000, 26)
		f = &builtinCastDecimalAsTimeSig{base}
	case tipb.ScalarFuncSig_CastDurationAsTime:
		trace_util_0.Count(_distsql_builtin_00000, 27)
		f = &builtinCastDurationAsTimeSig{base}
	case tipb.ScalarFuncSig_CastTimeAsTime:
		trace_util_0.Count(_distsql_builtin_00000, 28)
		f = &builtinCastTimeAsTimeSig{base}
	case tipb.ScalarFuncSig_CastStringAsTime:
		trace_util_0.Count(_distsql_builtin_00000, 29)
		f = &builtinCastStringAsTimeSig{base}
	case tipb.ScalarFuncSig_CastJsonAsTime:
		trace_util_0.Count(_distsql_builtin_00000, 30)
		f = &builtinCastJSONAsTimeSig{base}

	case tipb.ScalarFuncSig_CastIntAsString:
		trace_util_0.Count(_distsql_builtin_00000, 31)
		f = &builtinCastIntAsStringSig{base}
	case tipb.ScalarFuncSig_CastRealAsString:
		trace_util_0.Count(_distsql_builtin_00000, 32)
		f = &builtinCastRealAsStringSig{base}
	case tipb.ScalarFuncSig_CastDecimalAsString:
		trace_util_0.Count(_distsql_builtin_00000, 33)
		f = &builtinCastDecimalAsStringSig{base}
	case tipb.ScalarFuncSig_CastDurationAsString:
		trace_util_0.Count(_distsql_builtin_00000, 34)
		f = &builtinCastDurationAsStringSig{base}
	case tipb.ScalarFuncSig_CastTimeAsString:
		trace_util_0.Count(_distsql_builtin_00000, 35)
		f = &builtinCastTimeAsStringSig{base}
	case tipb.ScalarFuncSig_CastStringAsString:
		trace_util_0.Count(_distsql_builtin_00000, 36)
		f = &builtinCastStringAsStringSig{base}
	case tipb.ScalarFuncSig_CastJsonAsString:
		trace_util_0.Count(_distsql_builtin_00000, 37)
		f = &builtinCastJSONAsStringSig{base}

	case tipb.ScalarFuncSig_CastIntAsDuration:
		trace_util_0.Count(_distsql_builtin_00000, 38)
		f = &builtinCastIntAsDurationSig{base}
	case tipb.ScalarFuncSig_CastRealAsDuration:
		trace_util_0.Count(_distsql_builtin_00000, 39)
		f = &builtinCastRealAsDurationSig{base}
	case tipb.ScalarFuncSig_CastDecimalAsDuration:
		trace_util_0.Count(_distsql_builtin_00000, 40)
		f = &builtinCastDecimalAsDurationSig{base}
	case tipb.ScalarFuncSig_CastDurationAsDuration:
		trace_util_0.Count(_distsql_builtin_00000, 41)
		f = &builtinCastDurationAsDurationSig{base}
	case tipb.ScalarFuncSig_CastTimeAsDuration:
		trace_util_0.Count(_distsql_builtin_00000, 42)
		f = &builtinCastTimeAsDurationSig{base}
	case tipb.ScalarFuncSig_CastStringAsDuration:
		trace_util_0.Count(_distsql_builtin_00000, 43)
		f = &builtinCastStringAsDurationSig{base}
	case tipb.ScalarFuncSig_CastJsonAsDuration:
		trace_util_0.Count(_distsql_builtin_00000, 44)
		f = &builtinCastJSONAsDurationSig{base}

	case tipb.ScalarFuncSig_CastIntAsJson:
		trace_util_0.Count(_distsql_builtin_00000, 45)
		f = &builtinCastIntAsJSONSig{base}
	case tipb.ScalarFuncSig_CastRealAsJson:
		trace_util_0.Count(_distsql_builtin_00000, 46)
		f = &builtinCastRealAsJSONSig{base}
	case tipb.ScalarFuncSig_CastDecimalAsJson:
		trace_util_0.Count(_distsql_builtin_00000, 47)
		f = &builtinCastDecimalAsJSONSig{base}
	case tipb.ScalarFuncSig_CastTimeAsJson:
		trace_util_0.Count(_distsql_builtin_00000, 48)
		f = &builtinCastTimeAsJSONSig{base}
	case tipb.ScalarFuncSig_CastDurationAsJson:
		trace_util_0.Count(_distsql_builtin_00000, 49)
		f = &builtinCastDurationAsJSONSig{base}
	case tipb.ScalarFuncSig_CastStringAsJson:
		trace_util_0.Count(_distsql_builtin_00000, 50)
		f = &builtinCastStringAsJSONSig{base}
	case tipb.ScalarFuncSig_CastJsonAsJson:
		trace_util_0.Count(_distsql_builtin_00000, 51)
		f = &builtinCastJSONAsJSONSig{base}

	case tipb.ScalarFuncSig_GTInt:
		trace_util_0.Count(_distsql_builtin_00000, 52)
		f = &builtinGTIntSig{base}
	case tipb.ScalarFuncSig_GEInt:
		trace_util_0.Count(_distsql_builtin_00000, 53)
		f = &builtinGEIntSig{base}
	case tipb.ScalarFuncSig_LTInt:
		trace_util_0.Count(_distsql_builtin_00000, 54)
		f = &builtinLTIntSig{base}
	case tipb.ScalarFuncSig_LEInt:
		trace_util_0.Count(_distsql_builtin_00000, 55)
		f = &builtinLEIntSig{base}
	case tipb.ScalarFuncSig_EQInt:
		trace_util_0.Count(_distsql_builtin_00000, 56)
		f = &builtinEQIntSig{base}
	case tipb.ScalarFuncSig_NEInt:
		trace_util_0.Count(_distsql_builtin_00000, 57)
		f = &builtinNEIntSig{base}
	case tipb.ScalarFuncSig_NullEQInt:
		trace_util_0.Count(_distsql_builtin_00000, 58)
		f = &builtinNullEQIntSig{base}

	case tipb.ScalarFuncSig_GTReal:
		trace_util_0.Count(_distsql_builtin_00000, 59)
		f = &builtinGTRealSig{base}
	case tipb.ScalarFuncSig_GEReal:
		trace_util_0.Count(_distsql_builtin_00000, 60)
		f = &builtinGERealSig{base}
	case tipb.ScalarFuncSig_LTReal:
		trace_util_0.Count(_distsql_builtin_00000, 61)
		f = &builtinLTRealSig{base}
	case tipb.ScalarFuncSig_LEReal:
		trace_util_0.Count(_distsql_builtin_00000, 62)
		f = &builtinLERealSig{base}
	case tipb.ScalarFuncSig_EQReal:
		trace_util_0.Count(_distsql_builtin_00000, 63)
		f = &builtinEQRealSig{base}
	case tipb.ScalarFuncSig_NEReal:
		trace_util_0.Count(_distsql_builtin_00000, 64)
		f = &builtinNERealSig{base}
	case tipb.ScalarFuncSig_NullEQReal:
		trace_util_0.Count(_distsql_builtin_00000, 65)
		f = &builtinNullEQRealSig{base}

	case tipb.ScalarFuncSig_GTDecimal:
		trace_util_0.Count(_distsql_builtin_00000, 66)
		f = &builtinGTDecimalSig{base}
	case tipb.ScalarFuncSig_GEDecimal:
		trace_util_0.Count(_distsql_builtin_00000, 67)
		f = &builtinGEDecimalSig{base}
	case tipb.ScalarFuncSig_LTDecimal:
		trace_util_0.Count(_distsql_builtin_00000, 68)
		f = &builtinLTDecimalSig{base}
	case tipb.ScalarFuncSig_LEDecimal:
		trace_util_0.Count(_distsql_builtin_00000, 69)
		f = &builtinLEDecimalSig{base}
	case tipb.ScalarFuncSig_EQDecimal:
		trace_util_0.Count(_distsql_builtin_00000, 70)
		f = &builtinEQDecimalSig{base}
	case tipb.ScalarFuncSig_NEDecimal:
		trace_util_0.Count(_distsql_builtin_00000, 71)
		f = &builtinNEDecimalSig{base}
	case tipb.ScalarFuncSig_NullEQDecimal:
		trace_util_0.Count(_distsql_builtin_00000, 72)
		f = &builtinNullEQDecimalSig{base}

	case tipb.ScalarFuncSig_GTTime:
		trace_util_0.Count(_distsql_builtin_00000, 73)
		f = &builtinGTTimeSig{base}
	case tipb.ScalarFuncSig_GETime:
		trace_util_0.Count(_distsql_builtin_00000, 74)
		f = &builtinGETimeSig{base}
	case tipb.ScalarFuncSig_LTTime:
		trace_util_0.Count(_distsql_builtin_00000, 75)
		f = &builtinLTTimeSig{base}
	case tipb.ScalarFuncSig_LETime:
		trace_util_0.Count(_distsql_builtin_00000, 76)
		f = &builtinLETimeSig{base}
	case tipb.ScalarFuncSig_EQTime:
		trace_util_0.Count(_distsql_builtin_00000, 77)
		f = &builtinEQTimeSig{base}
	case tipb.ScalarFuncSig_NETime:
		trace_util_0.Count(_distsql_builtin_00000, 78)
		f = &builtinNETimeSig{base}
	case tipb.ScalarFuncSig_NullEQTime:
		trace_util_0.Count(_distsql_builtin_00000, 79)
		f = &builtinNullEQTimeSig{base}

	case tipb.ScalarFuncSig_GTDuration:
		trace_util_0.Count(_distsql_builtin_00000, 80)
		f = &builtinGTDurationSig{base}
	case tipb.ScalarFuncSig_GEDuration:
		trace_util_0.Count(_distsql_builtin_00000, 81)
		f = &builtinGEDurationSig{base}
	case tipb.ScalarFuncSig_LTDuration:
		trace_util_0.Count(_distsql_builtin_00000, 82)
		f = &builtinLTDurationSig{base}
	case tipb.ScalarFuncSig_LEDuration:
		trace_util_0.Count(_distsql_builtin_00000, 83)
		f = &builtinLEDurationSig{base}
	case tipb.ScalarFuncSig_EQDuration:
		trace_util_0.Count(_distsql_builtin_00000, 84)
		f = &builtinEQDurationSig{base}
	case tipb.ScalarFuncSig_NEDuration:
		trace_util_0.Count(_distsql_builtin_00000, 85)
		f = &builtinNEDurationSig{base}
	case tipb.ScalarFuncSig_NullEQDuration:
		trace_util_0.Count(_distsql_builtin_00000, 86)
		f = &builtinNullEQDurationSig{base}

	case tipb.ScalarFuncSig_GTString:
		trace_util_0.Count(_distsql_builtin_00000, 87)
		f = &builtinGTStringSig{base}
	case tipb.ScalarFuncSig_GEString:
		trace_util_0.Count(_distsql_builtin_00000, 88)
		f = &builtinGEStringSig{base}
	case tipb.ScalarFuncSig_LTString:
		trace_util_0.Count(_distsql_builtin_00000, 89)
		f = &builtinLTStringSig{base}
	case tipb.ScalarFuncSig_LEString:
		trace_util_0.Count(_distsql_builtin_00000, 90)
		f = &builtinLEStringSig{base}
	case tipb.ScalarFuncSig_EQString:
		trace_util_0.Count(_distsql_builtin_00000, 91)
		f = &builtinEQStringSig{base}
	case tipb.ScalarFuncSig_NEString:
		trace_util_0.Count(_distsql_builtin_00000, 92)
		f = &builtinNEStringSig{base}
	case tipb.ScalarFuncSig_NullEQString:
		trace_util_0.Count(_distsql_builtin_00000, 93)
		f = &builtinNullEQStringSig{base}

	case tipb.ScalarFuncSig_GTJson:
		trace_util_0.Count(_distsql_builtin_00000, 94)
		f = &builtinGTJSONSig{base}
	case tipb.ScalarFuncSig_GEJson:
		trace_util_0.Count(_distsql_builtin_00000, 95)
		f = &builtinGEJSONSig{base}
	case tipb.ScalarFuncSig_LTJson:
		trace_util_0.Count(_distsql_builtin_00000, 96)
		f = &builtinLTJSONSig{base}
	case tipb.ScalarFuncSig_LEJson:
		trace_util_0.Count(_distsql_builtin_00000, 97)
		f = &builtinLEJSONSig{base}
	case tipb.ScalarFuncSig_EQJson:
		trace_util_0.Count(_distsql_builtin_00000, 98)
		f = &builtinEQJSONSig{base}
	case tipb.ScalarFuncSig_NEJson:
		trace_util_0.Count(_distsql_builtin_00000, 99)
		f = &builtinNEJSONSig{base}
	case tipb.ScalarFuncSig_NullEQJson:
		trace_util_0.Count(_distsql_builtin_00000, 100)
		f = &builtinNullEQJSONSig{base}

	case tipb.ScalarFuncSig_PlusInt:
		trace_util_0.Count(_distsql_builtin_00000, 101)
		f = &builtinArithmeticPlusIntSig{base}
	case tipb.ScalarFuncSig_PlusDecimal:
		trace_util_0.Count(_distsql_builtin_00000, 102)
		f = &builtinArithmeticPlusDecimalSig{base}
	case tipb.ScalarFuncSig_PlusReal:
		trace_util_0.Count(_distsql_builtin_00000, 103)
		f = &builtinArithmeticPlusRealSig{base}
	case tipb.ScalarFuncSig_MinusInt:
		trace_util_0.Count(_distsql_builtin_00000, 104)
		f = &builtinArithmeticMinusIntSig{base}
	case tipb.ScalarFuncSig_MinusDecimal:
		trace_util_0.Count(_distsql_builtin_00000, 105)
		f = &builtinArithmeticMinusDecimalSig{base}
	case tipb.ScalarFuncSig_MinusReal:
		trace_util_0.Count(_distsql_builtin_00000, 106)
		f = &builtinArithmeticMinusRealSig{base}
	case tipb.ScalarFuncSig_MultiplyInt:
		trace_util_0.Count(_distsql_builtin_00000, 107)
		f = &builtinArithmeticMultiplyIntSig{base}
	case tipb.ScalarFuncSig_MultiplyDecimal:
		trace_util_0.Count(_distsql_builtin_00000, 108)
		f = &builtinArithmeticMultiplyDecimalSig{base}
	case tipb.ScalarFuncSig_MultiplyReal:
		trace_util_0.Count(_distsql_builtin_00000, 109)
		f = &builtinArithmeticMultiplyRealSig{base}
	case tipb.ScalarFuncSig_DivideDecimal:
		trace_util_0.Count(_distsql_builtin_00000, 110)
		f = &builtinArithmeticDivideDecimalSig{base}
	case tipb.ScalarFuncSig_DivideReal:
		trace_util_0.Count(_distsql_builtin_00000, 111)
		f = &builtinArithmeticDivideRealSig{base}
	case tipb.ScalarFuncSig_AbsInt:
		trace_util_0.Count(_distsql_builtin_00000, 112)
		f = &builtinAbsIntSig{base}
	case tipb.ScalarFuncSig_AbsUInt:
		trace_util_0.Count(_distsql_builtin_00000, 113)
		f = &builtinAbsUIntSig{base}
	case tipb.ScalarFuncSig_AbsReal:
		trace_util_0.Count(_distsql_builtin_00000, 114)
		f = &builtinAbsRealSig{base}
	case tipb.ScalarFuncSig_AbsDecimal:
		trace_util_0.Count(_distsql_builtin_00000, 115)
		f = &builtinAbsDecSig{base}
	case tipb.ScalarFuncSig_CeilIntToInt:
		trace_util_0.Count(_distsql_builtin_00000, 116)
		f = &builtinCeilIntToIntSig{base}
	case tipb.ScalarFuncSig_CeilIntToDec:
		trace_util_0.Count(_distsql_builtin_00000, 117)
		f = &builtinCeilIntToDecSig{base}
	case tipb.ScalarFuncSig_CeilDecToInt:
		trace_util_0.Count(_distsql_builtin_00000, 118)
		f = &builtinCeilDecToIntSig{base}
	case tipb.ScalarFuncSig_CeilDecToDec:
		trace_util_0.Count(_distsql_builtin_00000, 119)
		f = &builtinCeilDecToDecSig{base}
	case tipb.ScalarFuncSig_CeilReal:
		trace_util_0.Count(_distsql_builtin_00000, 120)
		f = &builtinCeilRealSig{base}
	case tipb.ScalarFuncSig_FloorIntToInt:
		trace_util_0.Count(_distsql_builtin_00000, 121)
		f = &builtinFloorIntToIntSig{base}
	case tipb.ScalarFuncSig_FloorIntToDec:
		trace_util_0.Count(_distsql_builtin_00000, 122)
		f = &builtinFloorIntToDecSig{base}
	case tipb.ScalarFuncSig_FloorDecToInt:
		trace_util_0.Count(_distsql_builtin_00000, 123)
		f = &builtinFloorDecToIntSig{base}
	case tipb.ScalarFuncSig_FloorDecToDec:
		trace_util_0.Count(_distsql_builtin_00000, 124)
		f = &builtinFloorDecToDecSig{base}
	case tipb.ScalarFuncSig_FloorReal:
		trace_util_0.Count(_distsql_builtin_00000, 125)
		f = &builtinFloorRealSig{base}

	case tipb.ScalarFuncSig_LogicalAnd:
		trace_util_0.Count(_distsql_builtin_00000, 126)
		f = &builtinLogicAndSig{base}
	case tipb.ScalarFuncSig_LogicalOr:
		trace_util_0.Count(_distsql_builtin_00000, 127)
		f = &builtinLogicOrSig{base}
	case tipb.ScalarFuncSig_LogicalXor:
		trace_util_0.Count(_distsql_builtin_00000, 128)
		f = &builtinLogicXorSig{base}
	case tipb.ScalarFuncSig_BitAndSig:
		trace_util_0.Count(_distsql_builtin_00000, 129)
		f = &builtinBitAndSig{base}
	case tipb.ScalarFuncSig_BitOrSig:
		trace_util_0.Count(_distsql_builtin_00000, 130)
		f = &builtinBitOrSig{base}
	case tipb.ScalarFuncSig_BitXorSig:
		trace_util_0.Count(_distsql_builtin_00000, 131)
		f = &builtinBitXorSig{base}
	case tipb.ScalarFuncSig_BitNegSig:
		trace_util_0.Count(_distsql_builtin_00000, 132)
		f = &builtinBitNegSig{base}

	case tipb.ScalarFuncSig_UnaryNot:
		trace_util_0.Count(_distsql_builtin_00000, 133)
		f = &builtinUnaryNotSig{base}
	case tipb.ScalarFuncSig_UnaryMinusInt:
		trace_util_0.Count(_distsql_builtin_00000, 134)
		f = &builtinUnaryMinusIntSig{base}
	case tipb.ScalarFuncSig_UnaryMinusReal:
		trace_util_0.Count(_distsql_builtin_00000, 135)
		f = &builtinUnaryMinusRealSig{base}
	case tipb.ScalarFuncSig_UnaryMinusDecimal:
		trace_util_0.Count(_distsql_builtin_00000, 136)
		f = &builtinUnaryMinusDecimalSig{base, false}

	case tipb.ScalarFuncSig_DecimalIsNull:
		trace_util_0.Count(_distsql_builtin_00000, 137)
		f = &builtinDecimalIsNullSig{base}
	case tipb.ScalarFuncSig_DurationIsNull:
		trace_util_0.Count(_distsql_builtin_00000, 138)
		f = &builtinDurationIsNullSig{base}
	case tipb.ScalarFuncSig_RealIsNull:
		trace_util_0.Count(_distsql_builtin_00000, 139)
		f = &builtinRealIsNullSig{base}
	case tipb.ScalarFuncSig_TimeIsNull:
		trace_util_0.Count(_distsql_builtin_00000, 140)
		f = &builtinTimeIsNullSig{base}
	case tipb.ScalarFuncSig_StringIsNull:
		trace_util_0.Count(_distsql_builtin_00000, 141)
		f = &builtinStringIsNullSig{base}
	case tipb.ScalarFuncSig_IntIsNull:
		trace_util_0.Count(_distsql_builtin_00000, 142)
		f = &builtinIntIsNullSig{base}

	case tipb.ScalarFuncSig_CoalesceDecimal:
		trace_util_0.Count(_distsql_builtin_00000, 143)
		f = &builtinCoalesceDecimalSig{base}
	case tipb.ScalarFuncSig_CoalesceDuration:
		trace_util_0.Count(_distsql_builtin_00000, 144)
		f = &builtinCoalesceDurationSig{base}
	case tipb.ScalarFuncSig_CoalesceReal:
		trace_util_0.Count(_distsql_builtin_00000, 145)
		f = &builtinCoalesceRealSig{base}
	case tipb.ScalarFuncSig_CoalesceTime:
		trace_util_0.Count(_distsql_builtin_00000, 146)
		f = &builtinCoalesceTimeSig{base}
	case tipb.ScalarFuncSig_CoalesceString:
		trace_util_0.Count(_distsql_builtin_00000, 147)
		f = &builtinCoalesceStringSig{base}
	case tipb.ScalarFuncSig_CoalesceInt:
		trace_util_0.Count(_distsql_builtin_00000, 148)
		f = &builtinCoalesceIntSig{base}

	case tipb.ScalarFuncSig_CaseWhenJson:
		trace_util_0.Count(_distsql_builtin_00000, 149)
		f = &builtinCaseWhenJSONSig{base}
	case tipb.ScalarFuncSig_CaseWhenDecimal:
		trace_util_0.Count(_distsql_builtin_00000, 150)
		f = &builtinCaseWhenDecimalSig{base}
	case tipb.ScalarFuncSig_CaseWhenDuration:
		trace_util_0.Count(_distsql_builtin_00000, 151)
		f = &builtinCaseWhenDurationSig{base}
	case tipb.ScalarFuncSig_CaseWhenReal:
		trace_util_0.Count(_distsql_builtin_00000, 152)
		f = &builtinCaseWhenRealSig{base}
	case tipb.ScalarFuncSig_CaseWhenTime:
		trace_util_0.Count(_distsql_builtin_00000, 153)
		f = &builtinCaseWhenTimeSig{base}
	case tipb.ScalarFuncSig_CaseWhenString:
		trace_util_0.Count(_distsql_builtin_00000, 154)
		f = &builtinCaseWhenStringSig{base}
	case tipb.ScalarFuncSig_CaseWhenInt:
		trace_util_0.Count(_distsql_builtin_00000, 155)
		f = &builtinCaseWhenIntSig{base}

	case tipb.ScalarFuncSig_IntIsFalse:
		trace_util_0.Count(_distsql_builtin_00000, 156)
		f = &builtinIntIsFalseSig{base}
	case tipb.ScalarFuncSig_RealIsFalse:
		trace_util_0.Count(_distsql_builtin_00000, 157)
		f = &builtinRealIsFalseSig{base}
	case tipb.ScalarFuncSig_DecimalIsFalse:
		trace_util_0.Count(_distsql_builtin_00000, 158)
		f = &builtinDecimalIsFalseSig{base}
	case tipb.ScalarFuncSig_IntIsTrue:
		trace_util_0.Count(_distsql_builtin_00000, 159)
		f = &builtinIntIsTrueSig{base}
	case tipb.ScalarFuncSig_RealIsTrue:
		trace_util_0.Count(_distsql_builtin_00000, 160)
		f = &builtinRealIsTrueSig{base}
	case tipb.ScalarFuncSig_DecimalIsTrue:
		trace_util_0.Count(_distsql_builtin_00000, 161)
		f = &builtinDecimalIsTrueSig{base}

	case tipb.ScalarFuncSig_IfNullReal:
		trace_util_0.Count(_distsql_builtin_00000, 162)
		f = &builtinIfNullRealSig{base}
	case tipb.ScalarFuncSig_IfNullInt:
		trace_util_0.Count(_distsql_builtin_00000, 163)
		f = &builtinIfNullIntSig{base}
	case tipb.ScalarFuncSig_IfNullDecimal:
		trace_util_0.Count(_distsql_builtin_00000, 164)
		f = &builtinIfNullDecimalSig{base}
	case tipb.ScalarFuncSig_IfNullString:
		trace_util_0.Count(_distsql_builtin_00000, 165)
		f = &builtinIfNullStringSig{base}
	case tipb.ScalarFuncSig_IfNullTime:
		trace_util_0.Count(_distsql_builtin_00000, 166)
		f = &builtinIfNullTimeSig{base}
	case tipb.ScalarFuncSig_IfNullDuration:
		trace_util_0.Count(_distsql_builtin_00000, 167)
		f = &builtinIfNullDurationSig{base}
	case tipb.ScalarFuncSig_IfNullJson:
		trace_util_0.Count(_distsql_builtin_00000, 168)
		f = &builtinIfNullJSONSig{base}
	case tipb.ScalarFuncSig_IfReal:
		trace_util_0.Count(_distsql_builtin_00000, 169)
		f = &builtinIfRealSig{base}
	case tipb.ScalarFuncSig_IfInt:
		trace_util_0.Count(_distsql_builtin_00000, 170)
		f = &builtinIfIntSig{base}
	case tipb.ScalarFuncSig_IfDecimal:
		trace_util_0.Count(_distsql_builtin_00000, 171)
		f = &builtinIfDecimalSig{base}
	case tipb.ScalarFuncSig_IfString:
		trace_util_0.Count(_distsql_builtin_00000, 172)
		f = &builtinIfStringSig{base}
	case tipb.ScalarFuncSig_IfTime:
		trace_util_0.Count(_distsql_builtin_00000, 173)
		f = &builtinIfTimeSig{base}
	case tipb.ScalarFuncSig_IfDuration:
		trace_util_0.Count(_distsql_builtin_00000, 174)
		f = &builtinIfDurationSig{base}
	case tipb.ScalarFuncSig_IfJson:
		trace_util_0.Count(_distsql_builtin_00000, 175)
		f = &builtinIfJSONSig{base}

	case tipb.ScalarFuncSig_JsonTypeSig:
		trace_util_0.Count(_distsql_builtin_00000, 176)
		f = &builtinJSONTypeSig{base}
	case tipb.ScalarFuncSig_JsonQuoteSig:
		trace_util_0.Count(_distsql_builtin_00000, 177)
		f = &builtinQuoteSig{base}
	case tipb.ScalarFuncSig_JsonUnquoteSig:
		trace_util_0.Count(_distsql_builtin_00000, 178)
		f = &builtinJSONUnquoteSig{base}
	case tipb.ScalarFuncSig_JsonArraySig:
		trace_util_0.Count(_distsql_builtin_00000, 179)
		f = &builtinJSONArraySig{base}
	case tipb.ScalarFuncSig_JsonArrayAppendSig:
		trace_util_0.Count(_distsql_builtin_00000, 180)
		f = &builtinJSONArrayAppendSig{base}
	case tipb.ScalarFuncSig_JsonObjectSig:
		trace_util_0.Count(_distsql_builtin_00000, 181)
		f = &builtinJSONObjectSig{base}
	case tipb.ScalarFuncSig_JsonExtractSig:
		trace_util_0.Count(_distsql_builtin_00000, 182)
		f = &builtinJSONExtractSig{base}
	case tipb.ScalarFuncSig_JsonSetSig:
		trace_util_0.Count(_distsql_builtin_00000, 183)
		f = &builtinJSONSetSig{base}
	case tipb.ScalarFuncSig_JsonInsertSig:
		trace_util_0.Count(_distsql_builtin_00000, 184)
		f = &builtinJSONInsertSig{base}
	case tipb.ScalarFuncSig_JsonReplaceSig:
		trace_util_0.Count(_distsql_builtin_00000, 185)
		f = &builtinJSONReplaceSig{base}
	case tipb.ScalarFuncSig_JsonRemoveSig:
		trace_util_0.Count(_distsql_builtin_00000, 186)
		f = &builtinJSONRemoveSig{base}
	case tipb.ScalarFuncSig_JsonMergeSig:
		trace_util_0.Count(_distsql_builtin_00000, 187)
		f = &builtinJSONMergeSig{base}
	case tipb.ScalarFuncSig_JsonContainsSig:
		trace_util_0.Count(_distsql_builtin_00000, 188)
		f = &builtinJSONContainsSig{base}
	case tipb.ScalarFuncSig_LikeSig:
		trace_util_0.Count(_distsql_builtin_00000, 189)
		f = &builtinLikeSig{base}
	case tipb.ScalarFuncSig_JsonLengthSig:
		trace_util_0.Count(_distsql_builtin_00000, 190)
		f = &builtinJSONLengthSig{base}
	case tipb.ScalarFuncSig_JsonDepthSig:
		trace_util_0.Count(_distsql_builtin_00000, 191)
		f = &builtinJSONDepthSig{base}
	case tipb.ScalarFuncSig_JsonSearchSig:
		trace_util_0.Count(_distsql_builtin_00000, 192)
		f = &builtinJSONSearchSig{base}

	case tipb.ScalarFuncSig_InInt:
		trace_util_0.Count(_distsql_builtin_00000, 193)
		f = &builtinInIntSig{base}
	case tipb.ScalarFuncSig_InReal:
		trace_util_0.Count(_distsql_builtin_00000, 194)
		f = &builtinInRealSig{base}
	case tipb.ScalarFuncSig_InDecimal:
		trace_util_0.Count(_distsql_builtin_00000, 195)
		f = &builtinInDecimalSig{base}
	case tipb.ScalarFuncSig_InString:
		trace_util_0.Count(_distsql_builtin_00000, 196)
		f = &builtinInStringSig{base}
	case tipb.ScalarFuncSig_InTime:
		trace_util_0.Count(_distsql_builtin_00000, 197)
		f = &builtinInTimeSig{base}
	case tipb.ScalarFuncSig_InDuration:
		trace_util_0.Count(_distsql_builtin_00000, 198)
		f = &builtinInDurationSig{base}
	case tipb.ScalarFuncSig_InJson:
		trace_util_0.Count(_distsql_builtin_00000, 199)
		f = &builtinInJSONSig{base}

	case tipb.ScalarFuncSig_DateFormatSig:
		trace_util_0.Count(_distsql_builtin_00000, 200)
		f = &builtinDateFormatSig{base}

	default:
		trace_util_0.Count(_distsql_builtin_00000, 201)
		e = errFunctionNotExists.GenWithStackByArgs("FUNCTION", sigCode)
		return nil, e
	}
	trace_util_0.Count(_distsql_builtin_00000, 2)
	return f, nil
}

func newDistSQLFunctionBySig(sc *stmtctx.StatementContext, sigCode tipb.ScalarFuncSig, tp *tipb.FieldType, args []Expression) (Expression, error) {
	trace_util_0.Count(_distsql_builtin_00000, 202)
	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx = sc
	f, err := getSignatureByPB(ctx, sigCode, tp, args)
	if err != nil {
		trace_util_0.Count(_distsql_builtin_00000, 204)
		return nil, err
	}
	trace_util_0.Count(_distsql_builtin_00000, 203)
	return &ScalarFunction{
		FuncName: model.NewCIStr(fmt.Sprintf("sig_%T", f)),
		Function: f,
		RetType:  f.getRetTp(),
	}, nil
}

// PBToExpr converts pb structure to expression.
func PBToExpr(expr *tipb.Expr, tps []*types.FieldType, sc *stmtctx.StatementContext) (Expression, error) {
	trace_util_0.Count(_distsql_builtin_00000, 205)
	switch expr.Tp {
	case tipb.ExprType_ColumnRef:
		trace_util_0.Count(_distsql_builtin_00000, 209)
		_, offset, err := codec.DecodeInt(expr.Val)
		if err != nil {
			trace_util_0.Count(_distsql_builtin_00000, 222)
			return nil, err
		}
		trace_util_0.Count(_distsql_builtin_00000, 210)
		return &Column{Index: int(offset), RetType: tps[offset]}, nil
	case tipb.ExprType_Null:
		trace_util_0.Count(_distsql_builtin_00000, 211)
		return &Constant{Value: types.Datum{}, RetType: types.NewFieldType(mysql.TypeNull)}, nil
	case tipb.ExprType_Int64:
		trace_util_0.Count(_distsql_builtin_00000, 212)
		return convertInt(expr.Val)
	case tipb.ExprType_Uint64:
		trace_util_0.Count(_distsql_builtin_00000, 213)
		return convertUint(expr.Val)
	case tipb.ExprType_String:
		trace_util_0.Count(_distsql_builtin_00000, 214)
		return convertString(expr.Val)
	case tipb.ExprType_Bytes:
		trace_util_0.Count(_distsql_builtin_00000, 215)
		return &Constant{Value: types.NewBytesDatum(expr.Val), RetType: types.NewFieldType(mysql.TypeString)}, nil
	case tipb.ExprType_Float32:
		trace_util_0.Count(_distsql_builtin_00000, 216)
		return convertFloat(expr.Val, true)
	case tipb.ExprType_Float64:
		trace_util_0.Count(_distsql_builtin_00000, 217)
		return convertFloat(expr.Val, false)
	case tipb.ExprType_MysqlDecimal:
		trace_util_0.Count(_distsql_builtin_00000, 218)
		return convertDecimal(expr.Val)
	case tipb.ExprType_MysqlDuration:
		trace_util_0.Count(_distsql_builtin_00000, 219)
		return convertDuration(expr.Val)
	case tipb.ExprType_MysqlTime:
		trace_util_0.Count(_distsql_builtin_00000, 220)
		return convertTime(expr.Val, expr.FieldType, sc.TimeZone)
	case tipb.ExprType_MysqlJson:
		trace_util_0.Count(_distsql_builtin_00000, 221)
		return convertJSON(expr.Val)
	}
	trace_util_0.Count(_distsql_builtin_00000, 206)
	if expr.Tp != tipb.ExprType_ScalarFunc {
		trace_util_0.Count(_distsql_builtin_00000, 223)
		panic("should be a tipb.ExprType_ScalarFunc")
	}
	// Then it must be a scalar function.
	trace_util_0.Count(_distsql_builtin_00000, 207)
	args := make([]Expression, 0, len(expr.Children))
	for _, child := range expr.Children {
		trace_util_0.Count(_distsql_builtin_00000, 224)
		if child.Tp == tipb.ExprType_ValueList {
			trace_util_0.Count(_distsql_builtin_00000, 227)
			results, err := decodeValueList(child.Val)
			if err != nil {
				trace_util_0.Count(_distsql_builtin_00000, 230)
				return nil, err
			}
			trace_util_0.Count(_distsql_builtin_00000, 228)
			if len(results) == 0 {
				trace_util_0.Count(_distsql_builtin_00000, 231)
				return &Constant{Value: types.NewDatum(false), RetType: types.NewFieldType(mysql.TypeLonglong)}, nil
			}
			trace_util_0.Count(_distsql_builtin_00000, 229)
			args = append(args, results...)
			continue
		}
		trace_util_0.Count(_distsql_builtin_00000, 225)
		arg, err := PBToExpr(child, tps, sc)
		if err != nil {
			trace_util_0.Count(_distsql_builtin_00000, 232)
			return nil, err
		}
		trace_util_0.Count(_distsql_builtin_00000, 226)
		args = append(args, arg)
	}
	trace_util_0.Count(_distsql_builtin_00000, 208)
	return newDistSQLFunctionBySig(sc, expr.Sig, expr.FieldType, args)
}

func convertTime(data []byte, ftPB *tipb.FieldType, tz *time.Location) (*Constant, error) {
	trace_util_0.Count(_distsql_builtin_00000, 233)
	ft := pbTypeToFieldType(ftPB)
	_, v, err := codec.DecodeUint(data)
	if err != nil {
		trace_util_0.Count(_distsql_builtin_00000, 237)
		return nil, err
	}
	trace_util_0.Count(_distsql_builtin_00000, 234)
	var t types.Time
	t.Type = ft.Tp
	t.Fsp = ft.Decimal
	err = t.FromPackedUint(v)
	if err != nil {
		trace_util_0.Count(_distsql_builtin_00000, 238)
		return nil, err
	}
	trace_util_0.Count(_distsql_builtin_00000, 235)
	if ft.Tp == mysql.TypeTimestamp && tz != time.UTC {
		trace_util_0.Count(_distsql_builtin_00000, 239)
		err = t.ConvertTimeZone(time.UTC, tz)
		if err != nil {
			trace_util_0.Count(_distsql_builtin_00000, 240)
			return nil, err
		}
	}
	trace_util_0.Count(_distsql_builtin_00000, 236)
	return &Constant{Value: types.NewTimeDatum(t), RetType: ft}, nil
}

func decodeValueList(data []byte) ([]Expression, error) {
	trace_util_0.Count(_distsql_builtin_00000, 241)
	if len(data) == 0 {
		trace_util_0.Count(_distsql_builtin_00000, 245)
		return nil, nil
	}
	trace_util_0.Count(_distsql_builtin_00000, 242)
	list, err := codec.Decode(data, 1)
	if err != nil {
		trace_util_0.Count(_distsql_builtin_00000, 246)
		return nil, err
	}
	trace_util_0.Count(_distsql_builtin_00000, 243)
	result := make([]Expression, 0, len(list))
	for _, value := range list {
		trace_util_0.Count(_distsql_builtin_00000, 247)
		result = append(result, &Constant{Value: value})
	}
	trace_util_0.Count(_distsql_builtin_00000, 244)
	return result, nil
}

func convertInt(val []byte) (*Constant, error) {
	trace_util_0.Count(_distsql_builtin_00000, 248)
	var d types.Datum
	_, i, err := codec.DecodeInt(val)
	if err != nil {
		trace_util_0.Count(_distsql_builtin_00000, 250)
		return nil, errors.Errorf("invalid int % x", val)
	}
	trace_util_0.Count(_distsql_builtin_00000, 249)
	d.SetInt64(i)
	return &Constant{Value: d, RetType: types.NewFieldType(mysql.TypeLonglong)}, nil
}

func convertUint(val []byte) (*Constant, error) {
	trace_util_0.Count(_distsql_builtin_00000, 251)
	var d types.Datum
	_, u, err := codec.DecodeUint(val)
	if err != nil {
		trace_util_0.Count(_distsql_builtin_00000, 253)
		return nil, errors.Errorf("invalid uint % x", val)
	}
	trace_util_0.Count(_distsql_builtin_00000, 252)
	d.SetUint64(u)
	return &Constant{Value: d, RetType: &types.FieldType{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag}}, nil
}

func convertString(val []byte) (*Constant, error) {
	trace_util_0.Count(_distsql_builtin_00000, 254)
	var d types.Datum
	d.SetBytesAsString(val)
	return &Constant{Value: d, RetType: types.NewFieldType(mysql.TypeVarString)}, nil
}

func convertFloat(val []byte, f32 bool) (*Constant, error) {
	trace_util_0.Count(_distsql_builtin_00000, 255)
	var d types.Datum
	_, f, err := codec.DecodeFloat(val)
	if err != nil {
		trace_util_0.Count(_distsql_builtin_00000, 258)
		return nil, errors.Errorf("invalid float % x", val)
	}
	trace_util_0.Count(_distsql_builtin_00000, 256)
	if f32 {
		trace_util_0.Count(_distsql_builtin_00000, 259)
		d.SetFloat32(float32(f))
	} else {
		trace_util_0.Count(_distsql_builtin_00000, 260)
		{
			d.SetFloat64(f)
		}
	}
	trace_util_0.Count(_distsql_builtin_00000, 257)
	return &Constant{Value: d, RetType: types.NewFieldType(mysql.TypeDouble)}, nil
}

func convertDecimal(val []byte) (*Constant, error) {
	trace_util_0.Count(_distsql_builtin_00000, 261)
	_, dec, precision, frac, err := codec.DecodeDecimal(val)
	var d types.Datum
	d.SetMysqlDecimal(dec)
	d.SetLength(precision)
	d.SetFrac(frac)
	if err != nil {
		trace_util_0.Count(_distsql_builtin_00000, 263)
		return nil, errors.Errorf("invalid decimal % x", val)
	}
	trace_util_0.Count(_distsql_builtin_00000, 262)
	return &Constant{Value: d, RetType: types.NewFieldType(mysql.TypeNewDecimal)}, nil
}

func convertDuration(val []byte) (*Constant, error) {
	trace_util_0.Count(_distsql_builtin_00000, 264)
	var d types.Datum
	_, i, err := codec.DecodeInt(val)
	if err != nil {
		trace_util_0.Count(_distsql_builtin_00000, 266)
		return nil, errors.Errorf("invalid duration %d", i)
	}
	trace_util_0.Count(_distsql_builtin_00000, 265)
	d.SetMysqlDuration(types.Duration{Duration: time.Duration(i), Fsp: types.MaxFsp})
	return &Constant{Value: d, RetType: types.NewFieldType(mysql.TypeDuration)}, nil
}

func convertJSON(val []byte) (*Constant, error) {
	trace_util_0.Count(_distsql_builtin_00000, 267)
	var d types.Datum
	_, d, err := codec.DecodeOne(val)
	if err != nil {
		trace_util_0.Count(_distsql_builtin_00000, 270)
		return nil, errors.Errorf("invalid json % x", val)
	}
	trace_util_0.Count(_distsql_builtin_00000, 268)
	if d.Kind() != types.KindMysqlJSON {
		trace_util_0.Count(_distsql_builtin_00000, 271)
		return nil, errors.Errorf("invalid Datum.Kind() %d", d.Kind())
	}
	trace_util_0.Count(_distsql_builtin_00000, 269)
	return &Constant{Value: d, RetType: types.NewFieldType(mysql.TypeJSON)}, nil
}

var _distsql_builtin_00000 = "expression/distsql_builtin.go"
