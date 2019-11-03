package aggfuncs

import (
	"encoding/binary"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/set"
)

type baseCount struct {
	baseAggFunc
}

type partialResult4Count = int64

func (e *baseCount) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_count_00000, 0)
	return PartialResult(new(partialResult4Count))
}

func (e *baseCount) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_count_00000, 1)
	p := (*partialResult4Count)(pr)
	*p = 0
}

func (e *baseCount) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_count_00000, 2)
	p := (*partialResult4Count)(pr)
	chk.AppendInt64(e.ordinal, *p)
	return nil
}

type countOriginal4Int struct {
	baseCount
}

func (e *countOriginal4Int) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_count_00000, 3)
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_count_00000, 5)
		_, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_count_00000, 8)
			return err
		}
		trace_util_0.Count(_func_count_00000, 6)
		if isNull {
			trace_util_0.Count(_func_count_00000, 9)
			continue
		}

		trace_util_0.Count(_func_count_00000, 7)
		*p++
	}

	trace_util_0.Count(_func_count_00000, 4)
	return nil
}

type countOriginal4Real struct {
	baseCount
}

func (e *countOriginal4Real) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_count_00000, 10)
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_count_00000, 12)
		_, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_count_00000, 15)
			return err
		}
		trace_util_0.Count(_func_count_00000, 13)
		if isNull {
			trace_util_0.Count(_func_count_00000, 16)
			continue
		}

		trace_util_0.Count(_func_count_00000, 14)
		*p++
	}

	trace_util_0.Count(_func_count_00000, 11)
	return nil
}

type countOriginal4Decimal struct {
	baseCount
}

func (e *countOriginal4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_count_00000, 17)
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_count_00000, 19)
		_, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_count_00000, 22)
			return err
		}
		trace_util_0.Count(_func_count_00000, 20)
		if isNull {
			trace_util_0.Count(_func_count_00000, 23)
			continue
		}

		trace_util_0.Count(_func_count_00000, 21)
		*p++
	}

	trace_util_0.Count(_func_count_00000, 18)
	return nil
}

type countOriginal4Time struct {
	baseCount
}

func (e *countOriginal4Time) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_count_00000, 24)
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_count_00000, 26)
		_, isNull, err := e.args[0].EvalTime(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_count_00000, 29)
			return err
		}
		trace_util_0.Count(_func_count_00000, 27)
		if isNull {
			trace_util_0.Count(_func_count_00000, 30)
			continue
		}

		trace_util_0.Count(_func_count_00000, 28)
		*p++
	}

	trace_util_0.Count(_func_count_00000, 25)
	return nil
}

type countOriginal4Duration struct {
	baseCount
}

func (e *countOriginal4Duration) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_count_00000, 31)
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_count_00000, 33)
		_, isNull, err := e.args[0].EvalDuration(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_count_00000, 36)
			return err
		}
		trace_util_0.Count(_func_count_00000, 34)
		if isNull {
			trace_util_0.Count(_func_count_00000, 37)
			continue
		}

		trace_util_0.Count(_func_count_00000, 35)
		*p++
	}

	trace_util_0.Count(_func_count_00000, 32)
	return nil
}

type countOriginal4JSON struct {
	baseCount
}

func (e *countOriginal4JSON) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_count_00000, 38)
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_count_00000, 40)
		_, isNull, err := e.args[0].EvalJSON(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_count_00000, 43)
			return err
		}
		trace_util_0.Count(_func_count_00000, 41)
		if isNull {
			trace_util_0.Count(_func_count_00000, 44)
			continue
		}

		trace_util_0.Count(_func_count_00000, 42)
		*p++
	}

	trace_util_0.Count(_func_count_00000, 39)
	return nil
}

type countOriginal4String struct {
	baseCount
}

func (e *countOriginal4String) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_count_00000, 45)
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_count_00000, 47)
		_, isNull, err := e.args[0].EvalString(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_count_00000, 50)
			return err
		}
		trace_util_0.Count(_func_count_00000, 48)
		if isNull {
			trace_util_0.Count(_func_count_00000, 51)
			continue
		}

		trace_util_0.Count(_func_count_00000, 49)
		*p++
	}

	trace_util_0.Count(_func_count_00000, 46)
	return nil
}

type countPartial struct {
	baseCount
}

func (e *countPartial) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_count_00000, 52)
	p := (*partialResult4Count)(pr)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_count_00000, 54)
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_count_00000, 57)
			return err
		}
		trace_util_0.Count(_func_count_00000, 55)
		if isNull {
			trace_util_0.Count(_func_count_00000, 58)
			continue
		}

		trace_util_0.Count(_func_count_00000, 56)
		*p += input
	}
	trace_util_0.Count(_func_count_00000, 53)
	return nil
}

func (*countPartial) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	trace_util_0.Count(_func_count_00000, 59)
	p1, p2 := (*partialResult4Count)(src), (*partialResult4Count)(dst)
	*p2 += *p1
	return nil
}

type countOriginalWithDistinct struct {
	baseCount
}

type partialResult4CountWithDistinct struct {
	count int64

	valSet set.StringSet
}

func (e *countOriginalWithDistinct) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_count_00000, 60)
	return PartialResult(&partialResult4CountWithDistinct{
		count:  0,
		valSet: set.NewStringSet(),
	})
}

func (e *countOriginalWithDistinct) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_count_00000, 61)
	p := (*partialResult4CountWithDistinct)(pr)
	p.count = 0
	p.valSet = set.NewStringSet()
}

func (e *countOriginalWithDistinct) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_count_00000, 62)
	p := (*partialResult4CountWithDistinct)(pr)
	chk.AppendInt64(e.ordinal, p.count)
	return nil
}

func (e *countOriginalWithDistinct) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (err error) {
	trace_util_0.Count(_func_count_00000, 63)
	p := (*partialResult4CountWithDistinct)(pr)

	encodedBytes := make([]byte, 0)
	// Decimal struct is the biggest type we will use.
	buf := make([]byte, types.MyDecimalStructSize)

	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_count_00000, 65)
		var hasNull, isNull bool
		encodedBytes = encodedBytes[:0]

		for i := 0; i < len(e.args) && !hasNull; i++ {
			trace_util_0.Count(_func_count_00000, 68)
			encodedBytes, isNull, err = e.evalAndEncode(sctx, e.args[i], row, buf, encodedBytes)
			if err != nil {
				trace_util_0.Count(_func_count_00000, 70)
				return
			}
			trace_util_0.Count(_func_count_00000, 69)
			if isNull {
				trace_util_0.Count(_func_count_00000, 71)
				hasNull = true
				break
			}
		}
		trace_util_0.Count(_func_count_00000, 66)
		encodedString := string(encodedBytes)
		if hasNull || p.valSet.Exist(encodedString) {
			trace_util_0.Count(_func_count_00000, 72)
			continue
		}
		trace_util_0.Count(_func_count_00000, 67)
		p.valSet.Insert(encodedString)
		p.count++
	}

	trace_util_0.Count(_func_count_00000, 64)
	return nil
}

// evalAndEncode eval one row with an expression and encode value to bytes.
func (e *countOriginalWithDistinct) evalAndEncode(
	sctx sessionctx.Context, arg expression.Expression,
	row chunk.Row, buf, encodedBytes []byte,
) (_ []byte, isNull bool, err error) {
	trace_util_0.Count(_func_count_00000, 73)
	switch tp := arg.GetType().EvalType(); tp {
	case types.ETInt:
		trace_util_0.Count(_func_count_00000, 75)
		var val int64
		val, isNull, err = arg.EvalInt(sctx, row)
		if err != nil || isNull {
			trace_util_0.Count(_func_count_00000, 90)
			break
		}
		trace_util_0.Count(_func_count_00000, 76)
		encodedBytes = appendInt64(encodedBytes, buf, val)
	case types.ETReal:
		trace_util_0.Count(_func_count_00000, 77)
		var val float64
		val, isNull, err = arg.EvalReal(sctx, row)
		if err != nil || isNull {
			trace_util_0.Count(_func_count_00000, 91)
			break
		}
		trace_util_0.Count(_func_count_00000, 78)
		encodedBytes = appendFloat64(encodedBytes, buf, val)
	case types.ETDecimal:
		trace_util_0.Count(_func_count_00000, 79)
		var val *types.MyDecimal
		val, isNull, err = arg.EvalDecimal(sctx, row)
		if err != nil || isNull {
			trace_util_0.Count(_func_count_00000, 92)
			break
		}
		trace_util_0.Count(_func_count_00000, 80)
		encodedBytes, err = appendDecimal(encodedBytes, val)
	case types.ETTimestamp, types.ETDatetime:
		trace_util_0.Count(_func_count_00000, 81)
		var val types.Time
		val, isNull, err = arg.EvalTime(sctx, row)
		if err != nil || isNull {
			trace_util_0.Count(_func_count_00000, 93)
			break
		}
		trace_util_0.Count(_func_count_00000, 82)
		encodedBytes = appendTime(encodedBytes, buf, val)
	case types.ETDuration:
		trace_util_0.Count(_func_count_00000, 83)
		var val types.Duration
		val, isNull, err = arg.EvalDuration(sctx, row)
		if err != nil || isNull {
			trace_util_0.Count(_func_count_00000, 94)
			break
		}
		trace_util_0.Count(_func_count_00000, 84)
		encodedBytes = appendDuration(encodedBytes, buf, val)
	case types.ETJson:
		trace_util_0.Count(_func_count_00000, 85)
		var val json.BinaryJSON
		val, isNull, err = arg.EvalJSON(sctx, row)
		if err != nil || isNull {
			trace_util_0.Count(_func_count_00000, 95)
			break
		}
		trace_util_0.Count(_func_count_00000, 86)
		encodedBytes = appendJSON(encodedBytes, buf, val)
	case types.ETString:
		trace_util_0.Count(_func_count_00000, 87)
		var val string
		val, isNull, err = arg.EvalString(sctx, row)
		if err != nil || isNull {
			trace_util_0.Count(_func_count_00000, 96)
			break
		}
		trace_util_0.Count(_func_count_00000, 88)
		encodedBytes = codec.EncodeBytes(encodedBytes, hack.Slice(val))
	default:
		trace_util_0.Count(_func_count_00000, 89)
		return nil, false, errors.Errorf("unsupported column type for encode %d", tp)
	}
	trace_util_0.Count(_func_count_00000, 74)
	return encodedBytes, isNull, err
}

func appendInt64(encodedBytes, buf []byte, val int64) []byte {
	trace_util_0.Count(_func_count_00000, 97)
	*(*int64)(unsafe.Pointer(&buf[0])) = val
	buf = buf[:8]
	encodedBytes = append(encodedBytes, buf...)
	return encodedBytes
}

func appendFloat64(encodedBytes, buf []byte, val float64) []byte {
	trace_util_0.Count(_func_count_00000, 98)
	*(*float64)(unsafe.Pointer(&buf[0])) = val
	buf = buf[:8]
	encodedBytes = append(encodedBytes, buf...)
	return encodedBytes
}

func appendDecimal(encodedBytes []byte, val *types.MyDecimal) ([]byte, error) {
	trace_util_0.Count(_func_count_00000, 99)
	hash, err := val.ToHashKey()
	encodedBytes = append(encodedBytes, hash...)
	return encodedBytes, err
}

func writeTime(buf []byte, t types.Time) {
	trace_util_0.Count(_func_count_00000, 100)
	binary.BigEndian.PutUint16(buf, uint16(t.Time.Year()))
	buf[2] = uint8(t.Time.Month())
	buf[3] = uint8(t.Time.Day())
	buf[4] = uint8(t.Time.Hour())
	buf[5] = uint8(t.Time.Minute())
	buf[6] = uint8(t.Time.Second())
	binary.BigEndian.PutUint32(buf[8:], uint32(t.Time.Microsecond()))
	buf[12] = t.Type
	buf[13] = uint8(t.Fsp)
}

func appendTime(encodedBytes, buf []byte, val types.Time) []byte {
	trace_util_0.Count(_func_count_00000, 101)
	writeTime(buf, val)
	buf = buf[:16]
	encodedBytes = append(encodedBytes, buf...)
	return encodedBytes
}

func appendDuration(encodedBytes, buf []byte, val types.Duration) []byte {
	trace_util_0.Count(_func_count_00000, 102)
	*(*types.Duration)(unsafe.Pointer(&buf[0])) = val
	buf = buf[:16]
	encodedBytes = append(encodedBytes, buf...)
	return encodedBytes
}

func appendJSON(encodedBytes, _ []byte, val json.BinaryJSON) []byte {
	trace_util_0.Count(_func_count_00000, 103)
	encodedBytes = append(encodedBytes, val.TypeCode)
	encodedBytes = append(encodedBytes, val.Value...)
	return encodedBytes
}

func appendString(encodedBytes, _ []byte, val string) []byte {
	trace_util_0.Count(_func_count_00000, 104)
	encodedBytes = append(encodedBytes, val...)
	return encodedBytes
}

var _func_count_00000 = "executor/aggfuncs/func_count.go"
