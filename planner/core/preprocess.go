// Copyright 2015 PingCAP, Inc.
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

package core

import (
	"fmt"
	"math"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/parser_driver"
)

// PreprocessOpt presents optional parameters to `Preprocess` method.
type PreprocessOpt func(*preprocessor)

// InPrepare is a PreprocessOpt that indicates preprocess is executing under prepare statement.
func InPrepare(p *preprocessor) {
	trace_util_0.Count(_preprocess_00000, 0)
	p.flag |= inPrepare
}

// InTxnRetry is a PreprocessOpt that indicates preprocess is executing under transaction retry.
func InTxnRetry(p *preprocessor) {
	trace_util_0.Count(_preprocess_00000, 1)
	p.flag |= inTxnRetry
}

// Preprocess resolves table names of the node, and checks some statements validation.
func Preprocess(ctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema, preprocessOpt ...PreprocessOpt) error {
	trace_util_0.Count(_preprocess_00000, 2)
	v := preprocessor{is: is, ctx: ctx, tableAliasInJoin: make([]map[string]interface{}, 0)}
	for _, optFn := range preprocessOpt {
		trace_util_0.Count(_preprocess_00000, 4)
		optFn(&v)
	}
	trace_util_0.Count(_preprocess_00000, 3)
	node.Accept(&v)
	return errors.Trace(v.err)
}

type preprocessorFlag uint8

const (
	// inPrepare is set when visiting in prepare statement.
	inPrepare preprocessorFlag = 1 << iota
	// inTxnRetry is set when visiting in transaction retry.
	inTxnRetry
	// inCreateOrDropTable is set when visiting create/drop table statement.
	inCreateOrDropTable
	// parentIsJoin is set when visiting node's parent is join.
	parentIsJoin
)

// preprocessor is an ast.Visitor that preprocess
// ast Nodes parsed from parser.
type preprocessor struct {
	is   infoschema.InfoSchema
	ctx  sessionctx.Context
	err  error
	flag preprocessorFlag

	// tableAliasInJoin is a stack that keeps the table alias names for joins.
	// len(tableAliasInJoin) may bigger than 1 because the left/right child of join may be subquery that contains `JOIN`
	tableAliasInJoin []map[string]interface{}
}

func (p *preprocessor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	trace_util_0.Count(_preprocess_00000, 5)
	switch node := in.(type) {
	case *ast.CreateTableStmt:
		trace_util_0.Count(_preprocess_00000, 7)
		p.flag |= inCreateOrDropTable
		p.checkCreateTableGrammar(node)
	case *ast.CreateViewStmt:
		trace_util_0.Count(_preprocess_00000, 8)
		p.flag |= inCreateOrDropTable
		p.checkCreateViewGrammar(node)
	case *ast.DropTableStmt:
		trace_util_0.Count(_preprocess_00000, 9)
		p.flag |= inCreateOrDropTable
		p.checkDropTableGrammar(node)
	case *ast.RenameTableStmt:
		trace_util_0.Count(_preprocess_00000, 10)
		p.flag |= inCreateOrDropTable
		p.checkRenameTableGrammar(node)
	case *ast.CreateIndexStmt:
		trace_util_0.Count(_preprocess_00000, 11)
		p.checkCreateIndexGrammar(node)
	case *ast.AlterTableStmt:
		trace_util_0.Count(_preprocess_00000, 12)
		p.resolveAlterTableStmt(node)
		p.checkAlterTableGrammar(node)
	case *ast.CreateDatabaseStmt:
		trace_util_0.Count(_preprocess_00000, 13)
		p.checkCreateDatabaseGrammar(node)
	case *ast.AlterDatabaseStmt:
		trace_util_0.Count(_preprocess_00000, 14)
		p.checkAlterDatabaseGrammar(node)
	case *ast.DropDatabaseStmt:
		trace_util_0.Count(_preprocess_00000, 15)
		p.checkDropDatabaseGrammar(node)
	case *ast.ShowStmt:
		trace_util_0.Count(_preprocess_00000, 16)
		p.resolveShowStmt(node)
	case *ast.UnionSelectList:
		trace_util_0.Count(_preprocess_00000, 17)
		p.checkUnionSelectList(node)
	case *ast.DeleteTableList:
		trace_util_0.Count(_preprocess_00000, 18)
		return in, true
	case *ast.Join:
		trace_util_0.Count(_preprocess_00000, 19)
		p.checkNonUniqTableAlias(node)
	case *ast.CreateBindingStmt:
		trace_util_0.Count(_preprocess_00000, 20)
		p.checkBindGrammar(node)
	case *ast.RecoverTableStmt:
		trace_util_0.Count(_preprocess_00000, 21)
		// The specified table in recover table statement maybe already been dropped.
		// So skip check table name here, otherwise, recover table [table_name] syntax will return
		// table not exists error. But recover table statement is use to recover the dropped table. So skip children here.
		return in, true
	default:
		trace_util_0.Count(_preprocess_00000, 22)
		p.flag &= ^parentIsJoin
	}
	trace_util_0.Count(_preprocess_00000, 6)
	return in, p.err != nil
}

func (p *preprocessor) checkBindGrammar(createBindingStmt *ast.CreateBindingStmt) {
	trace_util_0.Count(_preprocess_00000, 23)
	originSQL := parser.Normalize(createBindingStmt.OriginSel.(*ast.SelectStmt).Text())
	hintedSQL := parser.Normalize(createBindingStmt.HintedSel.(*ast.SelectStmt).Text())

	if originSQL != hintedSQL {
		trace_util_0.Count(_preprocess_00000, 24)
		p.err = errors.Errorf("hinted sql and origin sql don't match when hinted sql erase the hint info, after erase hint info, originSQL:%s, hintedSQL:%s", originSQL, hintedSQL)
	}
}

func (p *preprocessor) Leave(in ast.Node) (out ast.Node, ok bool) {
	trace_util_0.Count(_preprocess_00000, 25)
	switch x := in.(type) {
	case *ast.CreateTableStmt:
		trace_util_0.Count(_preprocess_00000, 27)
		p.flag &= ^inCreateOrDropTable
		p.checkAutoIncrement(x)
		p.checkContainDotColumn(x)
	case *ast.CreateViewStmt:
		trace_util_0.Count(_preprocess_00000, 28)
		p.flag &= ^inCreateOrDropTable
	case *ast.DropTableStmt, *ast.AlterTableStmt, *ast.RenameTableStmt:
		trace_util_0.Count(_preprocess_00000, 29)
		p.flag &= ^inCreateOrDropTable
	case *driver.ParamMarkerExpr:
		trace_util_0.Count(_preprocess_00000, 30)
		if p.flag&inPrepare == 0 {
			trace_util_0.Count(_preprocess_00000, 38)
			p.err = parser.ErrSyntax.GenWithStack("syntax error, unexpected '?'")
			return
		}
	case *ast.ExplainStmt:
		trace_util_0.Count(_preprocess_00000, 31)
		if _, ok := x.Stmt.(*ast.ShowStmt); ok {
			trace_util_0.Count(_preprocess_00000, 39)
			break
		}
		trace_util_0.Count(_preprocess_00000, 32)
		valid := false
		for i, length := 0, len(ast.ExplainFormats); i < length; i++ {
			trace_util_0.Count(_preprocess_00000, 40)
			if strings.ToLower(x.Format) == ast.ExplainFormats[i] {
				trace_util_0.Count(_preprocess_00000, 41)
				valid = true
				break
			}
		}
		trace_util_0.Count(_preprocess_00000, 33)
		if !valid {
			trace_util_0.Count(_preprocess_00000, 42)
			p.err = ErrUnknownExplainFormat.GenWithStackByArgs(x.Format)
		}
	case *ast.TableName:
		trace_util_0.Count(_preprocess_00000, 34)
		p.handleTableName(x)
	case *ast.Join:
		trace_util_0.Count(_preprocess_00000, 35)
		if len(p.tableAliasInJoin) > 0 {
			trace_util_0.Count(_preprocess_00000, 43)
			p.tableAliasInJoin = p.tableAliasInJoin[:len(p.tableAliasInJoin)-1]
		}
	case *ast.FuncCallExpr:
		trace_util_0.Count(_preprocess_00000, 36)
		// The arguments for builtin NAME_CONST should be constants
		// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_name-const for details
		if x.FnName.L == ast.NameConst {
			trace_util_0.Count(_preprocess_00000, 44)
			if len(x.Args) != 2 {
				trace_util_0.Count(_preprocess_00000, 46)
				p.err = expression.ErrIncorrectParameterCount.GenWithStackByArgs(x.FnName.L)
			} else {
				trace_util_0.Count(_preprocess_00000, 47)
				{
					_, isValueExpr1 := x.Args[0].(*driver.ValueExpr)
					_, isValueExpr2 := x.Args[1].(*driver.ValueExpr)
					if !isValueExpr1 || !isValueExpr2 {
						trace_util_0.Count(_preprocess_00000, 48)
						p.err = ErrWrongArguments.GenWithStackByArgs("NAME_CONST")
					}
				}
			}
			trace_util_0.Count(_preprocess_00000, 45)
			break
		}

		// no need sleep when retry transaction and avoid unexpect sleep caused by retry.
		trace_util_0.Count(_preprocess_00000, 37)
		if p.flag&inTxnRetry > 0 && x.FnName.L == ast.Sleep {
			trace_util_0.Count(_preprocess_00000, 49)
			if len(x.Args) == 1 {
				trace_util_0.Count(_preprocess_00000, 50)
				x.Args[0] = ast.NewValueExpr(0)
			}
		}
	}

	trace_util_0.Count(_preprocess_00000, 26)
	return in, p.err == nil
}

func checkAutoIncrementOp(colDef *ast.ColumnDef, num int) (bool, error) {
	trace_util_0.Count(_preprocess_00000, 51)
	var hasAutoIncrement bool

	if colDef.Options[num].Tp == ast.ColumnOptionAutoIncrement {
		trace_util_0.Count(_preprocess_00000, 54)
		hasAutoIncrement = true
		if len(colDef.Options) == num+1 {
			trace_util_0.Count(_preprocess_00000, 56)
			return hasAutoIncrement, nil
		}
		trace_util_0.Count(_preprocess_00000, 55)
		for _, op := range colDef.Options[num+1:] {
			trace_util_0.Count(_preprocess_00000, 57)
			if op.Tp == ast.ColumnOptionDefaultValue {
				trace_util_0.Count(_preprocess_00000, 58)
				if tmp, ok := op.Expr.(*driver.ValueExpr); ok {
					trace_util_0.Count(_preprocess_00000, 59)
					if !tmp.Datum.IsNull() {
						trace_util_0.Count(_preprocess_00000, 60)
						return hasAutoIncrement, errors.Errorf("Invalid default value for '%s'", colDef.Name.Name.O)
					}
				}
			}
		}
	}
	trace_util_0.Count(_preprocess_00000, 52)
	if colDef.Options[num].Tp == ast.ColumnOptionDefaultValue && len(colDef.Options) != num+1 {
		trace_util_0.Count(_preprocess_00000, 61)
		if tmp, ok := colDef.Options[num].Expr.(*driver.ValueExpr); ok {
			trace_util_0.Count(_preprocess_00000, 63)
			if tmp.Datum.IsNull() {
				trace_util_0.Count(_preprocess_00000, 64)
				return hasAutoIncrement, nil
			}
		}
		trace_util_0.Count(_preprocess_00000, 62)
		for _, op := range colDef.Options[num+1:] {
			trace_util_0.Count(_preprocess_00000, 65)
			if op.Tp == ast.ColumnOptionAutoIncrement {
				trace_util_0.Count(_preprocess_00000, 66)
				return hasAutoIncrement, errors.Errorf("Invalid default value for '%s'", colDef.Name.Name.O)
			}
		}
	}

	trace_util_0.Count(_preprocess_00000, 53)
	return hasAutoIncrement, nil
}

func isConstraintKeyTp(constraints []*ast.Constraint, colDef *ast.ColumnDef) bool {
	trace_util_0.Count(_preprocess_00000, 67)
	for _, c := range constraints {
		trace_util_0.Count(_preprocess_00000, 69)
		// If the constraint as follows: primary key(c1, c2)
		// we only support c1 column can be auto_increment.
		if colDef.Name.Name.L != c.Keys[0].Column.Name.L {
			trace_util_0.Count(_preprocess_00000, 71)
			continue
		}
		trace_util_0.Count(_preprocess_00000, 70)
		switch c.Tp {
		case ast.ConstraintPrimaryKey, ast.ConstraintKey, ast.ConstraintIndex,
			ast.ConstraintUniq, ast.ConstraintUniqIndex, ast.ConstraintUniqKey:
			trace_util_0.Count(_preprocess_00000, 72)
			return true
		}
	}

	trace_util_0.Count(_preprocess_00000, 68)
	return false
}

func (p *preprocessor) checkAutoIncrement(stmt *ast.CreateTableStmt) {
	trace_util_0.Count(_preprocess_00000, 73)
	var (
		isKey            bool
		count            int
		autoIncrementCol *ast.ColumnDef
	)

	for _, colDef := range stmt.Cols {
		trace_util_0.Count(_preprocess_00000, 79)
		var hasAutoIncrement bool
		for i, op := range colDef.Options {
			trace_util_0.Count(_preprocess_00000, 81)
			ok, err := checkAutoIncrementOp(colDef, i)
			if err != nil {
				trace_util_0.Count(_preprocess_00000, 84)
				p.err = err
				return
			}
			trace_util_0.Count(_preprocess_00000, 82)
			if ok {
				trace_util_0.Count(_preprocess_00000, 85)
				hasAutoIncrement = true
			}
			trace_util_0.Count(_preprocess_00000, 83)
			switch op.Tp {
			case ast.ColumnOptionPrimaryKey, ast.ColumnOptionUniqKey:
				trace_util_0.Count(_preprocess_00000, 86)
				isKey = true
			}
		}
		trace_util_0.Count(_preprocess_00000, 80)
		if hasAutoIncrement {
			trace_util_0.Count(_preprocess_00000, 87)
			count++
			autoIncrementCol = colDef
		}
	}

	trace_util_0.Count(_preprocess_00000, 74)
	if count < 1 {
		trace_util_0.Count(_preprocess_00000, 88)
		return
	}
	trace_util_0.Count(_preprocess_00000, 75)
	if !isKey {
		trace_util_0.Count(_preprocess_00000, 89)
		isKey = isConstraintKeyTp(stmt.Constraints, autoIncrementCol)
	}
	trace_util_0.Count(_preprocess_00000, 76)
	autoIncrementMustBeKey := true
	for _, opt := range stmt.Options {
		trace_util_0.Count(_preprocess_00000, 90)
		if opt.Tp == ast.TableOptionEngine && strings.EqualFold(opt.StrValue, "MyISAM") {
			trace_util_0.Count(_preprocess_00000, 91)
			autoIncrementMustBeKey = false
		}
	}
	trace_util_0.Count(_preprocess_00000, 77)
	if (autoIncrementMustBeKey && !isKey) || count > 1 {
		trace_util_0.Count(_preprocess_00000, 92)
		p.err = errors.New("Incorrect table definition; there can be only one auto column and it must be defined as a key")
	}

	trace_util_0.Count(_preprocess_00000, 78)
	switch autoIncrementCol.Tp.Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong,
		mysql.TypeFloat, mysql.TypeDouble, mysql.TypeLonglong, mysql.TypeInt24:
		trace_util_0.Count(_preprocess_00000, 93)
	default:
		trace_util_0.Count(_preprocess_00000, 94)
		p.err = errors.Errorf("Incorrect column specifier for column '%s'", autoIncrementCol.Name.Name.O)
	}
}

// checkUnionSelectList checks union's selectList.
// refer: https://dev.mysql.com/doc/refman/5.7/en/union.html
// "To apply ORDER BY or LIMIT to an individual SELECT, place the clause inside the parentheses that enclose the SELECT."
func (p *preprocessor) checkUnionSelectList(stmt *ast.UnionSelectList) {
	trace_util_0.Count(_preprocess_00000, 95)
	for _, sel := range stmt.Selects[:len(stmt.Selects)-1] {
		trace_util_0.Count(_preprocess_00000, 96)
		if sel.IsInBraces {
			trace_util_0.Count(_preprocess_00000, 99)
			continue
		}
		trace_util_0.Count(_preprocess_00000, 97)
		if sel.Limit != nil {
			trace_util_0.Count(_preprocess_00000, 100)
			p.err = ErrWrongUsage.GenWithStackByArgs("UNION", "LIMIT")
			return
		}
		trace_util_0.Count(_preprocess_00000, 98)
		if sel.OrderBy != nil {
			trace_util_0.Count(_preprocess_00000, 101)
			p.err = ErrWrongUsage.GenWithStackByArgs("UNION", "ORDER BY")
			return
		}
	}
}

func (p *preprocessor) checkCreateDatabaseGrammar(stmt *ast.CreateDatabaseStmt) {
	trace_util_0.Count(_preprocess_00000, 102)
	if isIncorrectName(stmt.Name) {
		trace_util_0.Count(_preprocess_00000, 103)
		p.err = ddl.ErrWrongDBName.GenWithStackByArgs(stmt.Name)
	}
}

func (p *preprocessor) checkAlterDatabaseGrammar(stmt *ast.AlterDatabaseStmt) {
	trace_util_0.Count(_preprocess_00000, 104)
	// for 'ALTER DATABASE' statement, database name can be empty to alter default database.
	if isIncorrectName(stmt.Name) && !stmt.AlterDefaultDatabase {
		trace_util_0.Count(_preprocess_00000, 105)
		p.err = ddl.ErrWrongDBName.GenWithStackByArgs(stmt.Name)
	}
}

func (p *preprocessor) checkDropDatabaseGrammar(stmt *ast.DropDatabaseStmt) {
	trace_util_0.Count(_preprocess_00000, 106)
	if isIncorrectName(stmt.Name) {
		trace_util_0.Count(_preprocess_00000, 107)
		p.err = ddl.ErrWrongDBName.GenWithStackByArgs(stmt.Name)
	}
}

func (p *preprocessor) checkCreateTableGrammar(stmt *ast.CreateTableStmt) {
	trace_util_0.Count(_preprocess_00000, 108)
	tName := stmt.Table.Name.String()
	if isIncorrectName(tName) {
		trace_util_0.Count(_preprocess_00000, 112)
		p.err = ddl.ErrWrongTableName.GenWithStackByArgs(tName)
		return
	}
	trace_util_0.Count(_preprocess_00000, 109)
	countPrimaryKey := 0
	for _, colDef := range stmt.Cols {
		trace_util_0.Count(_preprocess_00000, 113)
		if err := checkColumn(colDef); err != nil {
			trace_util_0.Count(_preprocess_00000, 116)
			p.err = err
			return
		}
		trace_util_0.Count(_preprocess_00000, 114)
		isPrimary, err := checkColumnOptions(colDef.Options)
		if err != nil {
			trace_util_0.Count(_preprocess_00000, 117)
			p.err = err
			return
		}
		trace_util_0.Count(_preprocess_00000, 115)
		countPrimaryKey += isPrimary
		if countPrimaryKey > 1 {
			trace_util_0.Count(_preprocess_00000, 118)
			p.err = infoschema.ErrMultiplePriKey
			return
		}
	}
	trace_util_0.Count(_preprocess_00000, 110)
	for _, constraint := range stmt.Constraints {
		trace_util_0.Count(_preprocess_00000, 119)
		switch tp := constraint.Tp; tp {
		case ast.ConstraintKey, ast.ConstraintIndex, ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
			trace_util_0.Count(_preprocess_00000, 120)
			err := checkIndexInfo(constraint.Name, constraint.Keys)
			if err != nil {
				trace_util_0.Count(_preprocess_00000, 123)
				p.err = err
				return
			}
		case ast.ConstraintPrimaryKey:
			trace_util_0.Count(_preprocess_00000, 121)
			if countPrimaryKey > 0 {
				trace_util_0.Count(_preprocess_00000, 124)
				p.err = infoschema.ErrMultiplePriKey
				return
			}
			trace_util_0.Count(_preprocess_00000, 122)
			countPrimaryKey++
			err := checkIndexInfo(constraint.Name, constraint.Keys)
			if err != nil {
				trace_util_0.Count(_preprocess_00000, 125)
				p.err = err
				return
			}
		}
	}
	trace_util_0.Count(_preprocess_00000, 111)
	if stmt.Select != nil {
		trace_util_0.Count(_preprocess_00000, 126)
		// FIXME: a temp error noticing 'not implemented' (issue 4754)
		p.err = errors.New("'CREATE TABLE ... SELECT' is not implemented yet")
		return
	} else {
		trace_util_0.Count(_preprocess_00000, 127)
		if len(stmt.Cols) == 0 && stmt.ReferTable == nil {
			trace_util_0.Count(_preprocess_00000, 128)
			p.err = ddl.ErrTableMustHaveColumns
			return
		}
	}
}

func (p *preprocessor) checkCreateViewGrammar(stmt *ast.CreateViewStmt) {
	trace_util_0.Count(_preprocess_00000, 129)
	vName := stmt.ViewName.Name.String()
	if isIncorrectName(vName) {
		trace_util_0.Count(_preprocess_00000, 131)
		p.err = ddl.ErrWrongTableName.GenWithStackByArgs(vName)
		return
	}
	trace_util_0.Count(_preprocess_00000, 130)
	for _, col := range stmt.Cols {
		trace_util_0.Count(_preprocess_00000, 132)
		if isIncorrectName(col.String()) {
			trace_util_0.Count(_preprocess_00000, 133)
			p.err = ddl.ErrWrongColumnName.GenWithStackByArgs(col)
			return
		}
	}
}

func (p *preprocessor) checkDropTableGrammar(stmt *ast.DropTableStmt) {
	trace_util_0.Count(_preprocess_00000, 134)
	for _, t := range stmt.Tables {
		trace_util_0.Count(_preprocess_00000, 135)
		if isIncorrectName(t.Name.String()) {
			trace_util_0.Count(_preprocess_00000, 136)
			p.err = ddl.ErrWrongTableName.GenWithStackByArgs(t.Name.String())
			return
		}
	}
}

func (p *preprocessor) checkNonUniqTableAlias(stmt *ast.Join) {
	trace_util_0.Count(_preprocess_00000, 137)
	if p.flag&parentIsJoin == 0 {
		trace_util_0.Count(_preprocess_00000, 141)
		p.tableAliasInJoin = append(p.tableAliasInJoin, make(map[string]interface{}))
	}
	trace_util_0.Count(_preprocess_00000, 138)
	tableAliases := p.tableAliasInJoin[len(p.tableAliasInJoin)-1]
	if err := isTableAliasDuplicate(stmt.Left, tableAliases); err != nil {
		trace_util_0.Count(_preprocess_00000, 142)
		p.err = err
		return
	}
	trace_util_0.Count(_preprocess_00000, 139)
	if err := isTableAliasDuplicate(stmt.Right, tableAliases); err != nil {
		trace_util_0.Count(_preprocess_00000, 143)
		p.err = err
		return
	}
	trace_util_0.Count(_preprocess_00000, 140)
	p.flag |= parentIsJoin
}

func isTableAliasDuplicate(node ast.ResultSetNode, tableAliases map[string]interface{}) error {
	trace_util_0.Count(_preprocess_00000, 144)
	if ts, ok := node.(*ast.TableSource); ok {
		trace_util_0.Count(_preprocess_00000, 146)
		tabName := ts.AsName
		if tabName.L == "" {
			trace_util_0.Count(_preprocess_00000, 149)
			if tableNode, ok := ts.Source.(*ast.TableName); ok {
				trace_util_0.Count(_preprocess_00000, 150)
				if tableNode.Schema.L != "" {
					trace_util_0.Count(_preprocess_00000, 151)
					tabName = model.NewCIStr(fmt.Sprintf("%s.%s", tableNode.Schema.L, tableNode.Name.L))
				} else {
					trace_util_0.Count(_preprocess_00000, 152)
					{
						tabName = tableNode.Name
					}
				}
			}
		}
		trace_util_0.Count(_preprocess_00000, 147)
		_, exists := tableAliases[tabName.L]
		if len(tabName.L) != 0 && exists {
			trace_util_0.Count(_preprocess_00000, 153)
			return ErrNonUniqTable.GenWithStackByArgs(tabName)
		}
		trace_util_0.Count(_preprocess_00000, 148)
		tableAliases[tabName.L] = nil
	}
	trace_util_0.Count(_preprocess_00000, 145)
	return nil
}

func checkColumnOptions(ops []*ast.ColumnOption) (int, error) {
	trace_util_0.Count(_preprocess_00000, 154)
	isPrimary, isGenerated, isStored := 0, 0, false

	for _, op := range ops {
		trace_util_0.Count(_preprocess_00000, 157)
		switch op.Tp {
		case ast.ColumnOptionPrimaryKey:
			trace_util_0.Count(_preprocess_00000, 158)
			isPrimary = 1
		case ast.ColumnOptionGenerated:
			trace_util_0.Count(_preprocess_00000, 159)
			isGenerated = 1
			isStored = op.Stored
		}
	}

	trace_util_0.Count(_preprocess_00000, 155)
	if isPrimary > 0 && isGenerated > 0 && !isStored {
		trace_util_0.Count(_preprocess_00000, 160)
		return isPrimary, ErrUnsupportedOnGeneratedColumn.GenWithStackByArgs("Defining a virtual generated column as primary key")
	}

	trace_util_0.Count(_preprocess_00000, 156)
	return isPrimary, nil
}

func (p *preprocessor) checkCreateIndexGrammar(stmt *ast.CreateIndexStmt) {
	trace_util_0.Count(_preprocess_00000, 161)
	tName := stmt.Table.Name.String()
	if isIncorrectName(tName) {
		trace_util_0.Count(_preprocess_00000, 163)
		p.err = ddl.ErrWrongTableName.GenWithStackByArgs(tName)
		return
	}
	trace_util_0.Count(_preprocess_00000, 162)
	p.err = checkIndexInfo(stmt.IndexName, stmt.IndexColNames)
}

func (p *preprocessor) checkRenameTableGrammar(stmt *ast.RenameTableStmt) {
	trace_util_0.Count(_preprocess_00000, 164)
	oldTable := stmt.OldTable.Name.String()
	newTable := stmt.NewTable.Name.String()

	if isIncorrectName(oldTable) {
		trace_util_0.Count(_preprocess_00000, 166)
		p.err = ddl.ErrWrongTableName.GenWithStackByArgs(oldTable)
		return
	}

	trace_util_0.Count(_preprocess_00000, 165)
	if isIncorrectName(newTable) {
		trace_util_0.Count(_preprocess_00000, 167)
		p.err = ddl.ErrWrongTableName.GenWithStackByArgs(newTable)
		return
	}
}

func (p *preprocessor) checkAlterTableGrammar(stmt *ast.AlterTableStmt) {
	trace_util_0.Count(_preprocess_00000, 168)
	tName := stmt.Table.Name.String()
	if isIncorrectName(tName) {
		trace_util_0.Count(_preprocess_00000, 170)
		p.err = ddl.ErrWrongTableName.GenWithStackByArgs(tName)
		return
	}
	trace_util_0.Count(_preprocess_00000, 169)
	specs := stmt.Specs
	for _, spec := range specs {
		trace_util_0.Count(_preprocess_00000, 171)
		if spec.NewTable != nil {
			trace_util_0.Count(_preprocess_00000, 174)
			ntName := spec.NewTable.Name.String()
			if isIncorrectName(ntName) {
				trace_util_0.Count(_preprocess_00000, 175)
				p.err = ddl.ErrWrongTableName.GenWithStackByArgs(ntName)
				return
			}
		}
		trace_util_0.Count(_preprocess_00000, 172)
		for _, colDef := range spec.NewColumns {
			trace_util_0.Count(_preprocess_00000, 176)
			if p.err = checkColumn(colDef); p.err != nil {
				trace_util_0.Count(_preprocess_00000, 177)
				return
			}
		}
		trace_util_0.Count(_preprocess_00000, 173)
		switch spec.Tp {
		case ast.AlterTableAddConstraint:
			trace_util_0.Count(_preprocess_00000, 178)
			switch spec.Constraint.Tp {
			case ast.ConstraintKey, ast.ConstraintIndex, ast.ConstraintUniq, ast.ConstraintUniqIndex,
				ast.ConstraintUniqKey:
				trace_util_0.Count(_preprocess_00000, 180)
				p.err = checkIndexInfo(spec.Constraint.Name, spec.Constraint.Keys)
				if p.err != nil {
					trace_util_0.Count(_preprocess_00000, 182)
					return
				}
			default:
				trace_util_0.Count(_preprocess_00000, 181)
				// Nothing to do now.
			}
		default:
			trace_util_0.Count(_preprocess_00000, 179)
			// Nothing to do now.
		}
	}
}

// checkDuplicateColumnName checks if index exists duplicated columns.
func checkDuplicateColumnName(indexColNames []*ast.IndexColName) error {
	trace_util_0.Count(_preprocess_00000, 183)
	colNames := make(map[string]struct{}, len(indexColNames))
	for _, indexColName := range indexColNames {
		trace_util_0.Count(_preprocess_00000, 185)
		name := indexColName.Column.Name
		if _, ok := colNames[name.L]; ok {
			trace_util_0.Count(_preprocess_00000, 187)
			return infoschema.ErrColumnExists.GenWithStackByArgs(name)
		}
		trace_util_0.Count(_preprocess_00000, 186)
		colNames[name.L] = struct{}{}
	}
	trace_util_0.Count(_preprocess_00000, 184)
	return nil
}

// checkIndexInfo checks index name and index column names.
func checkIndexInfo(indexName string, indexColNames []*ast.IndexColName) error {
	trace_util_0.Count(_preprocess_00000, 188)
	if strings.EqualFold(indexName, mysql.PrimaryKeyName) {
		trace_util_0.Count(_preprocess_00000, 191)
		return ddl.ErrWrongNameForIndex.GenWithStackByArgs(indexName)
	}
	trace_util_0.Count(_preprocess_00000, 189)
	if len(indexColNames) > mysql.MaxKeyParts {
		trace_util_0.Count(_preprocess_00000, 192)
		return infoschema.ErrTooManyKeyParts.GenWithStackByArgs(mysql.MaxKeyParts)
	}
	trace_util_0.Count(_preprocess_00000, 190)
	return checkDuplicateColumnName(indexColNames)
}

// checkColumn checks if the column definition is valid.
// See https://dev.mysql.com/doc/refman/5.7/en/storage-requirements.html
func checkColumn(colDef *ast.ColumnDef) error {
	trace_util_0.Count(_preprocess_00000, 193)
	// Check column name.
	cName := colDef.Name.Name.String()
	if isIncorrectName(cName) {
		trace_util_0.Count(_preprocess_00000, 199)
		return ddl.ErrWrongColumnName.GenWithStackByArgs(cName)
	}

	trace_util_0.Count(_preprocess_00000, 194)
	if isInvalidDefaultValue(colDef) {
		trace_util_0.Count(_preprocess_00000, 200)
		return types.ErrInvalidDefault.GenWithStackByArgs(colDef.Name.Name.O)
	}

	// Check column type.
	trace_util_0.Count(_preprocess_00000, 195)
	tp := colDef.Tp
	if tp == nil {
		trace_util_0.Count(_preprocess_00000, 201)
		return nil
	}
	trace_util_0.Count(_preprocess_00000, 196)
	if tp.Flen > math.MaxUint32 {
		trace_util_0.Count(_preprocess_00000, 202)
		return types.ErrTooBigDisplayWidth.GenWithStack("Display width out of range for column '%s' (max = %d)", colDef.Name.Name.O, math.MaxUint32)
	}

	trace_util_0.Count(_preprocess_00000, 197)
	switch tp.Tp {
	case mysql.TypeString:
		trace_util_0.Count(_preprocess_00000, 203)
		if tp.Flen != types.UnspecifiedLength && tp.Flen > mysql.MaxFieldCharLength {
			trace_util_0.Count(_preprocess_00000, 213)
			return types.ErrTooBigFieldLength.GenWithStack("Column length too big for column '%s' (max = %d); use BLOB or TEXT instead", colDef.Name.Name.O, mysql.MaxFieldCharLength)
		}
	case mysql.TypeVarchar:
		trace_util_0.Count(_preprocess_00000, 204)
		if len(tp.Charset) == 0 {
			trace_util_0.Count(_preprocess_00000, 214)
			// It's not easy to get the schema charset and table charset here.
			// The charset is determined by the order ColumnDefaultCharset --> TableDefaultCharset-->DatabaseDefaultCharset-->SystemDefaultCharset.
			// return nil, to make the check in the ddl.CreateTable.
			return nil
		}
		trace_util_0.Count(_preprocess_00000, 205)
		err := ddl.IsTooBigFieldLength(colDef.Tp.Flen, colDef.Name.Name.O, tp.Charset)
		if err != nil {
			trace_util_0.Count(_preprocess_00000, 215)
			return err
		}
	case mysql.TypeFloat, mysql.TypeDouble:
		trace_util_0.Count(_preprocess_00000, 206)
		if tp.Decimal > mysql.MaxFloatingTypeScale {
			trace_util_0.Count(_preprocess_00000, 216)
			return types.ErrTooBigScale.GenWithStackByArgs(tp.Decimal, colDef.Name.Name.O, mysql.MaxFloatingTypeScale)
		}
		trace_util_0.Count(_preprocess_00000, 207)
		if tp.Flen > mysql.MaxFloatingTypeWidth {
			trace_util_0.Count(_preprocess_00000, 217)
			return types.ErrTooBigPrecision.GenWithStackByArgs(tp.Flen, colDef.Name.Name.O, mysql.MaxFloatingTypeWidth)
		}
	case mysql.TypeSet:
		trace_util_0.Count(_preprocess_00000, 208)
		if len(tp.Elems) > mysql.MaxTypeSetMembers {
			trace_util_0.Count(_preprocess_00000, 218)
			return types.ErrTooBigSet.GenWithStack("Too many strings for column %s and SET", colDef.Name.Name.O)
		}
		// Check set elements. See https://dev.mysql.com/doc/refman/5.7/en/set.html .
		trace_util_0.Count(_preprocess_00000, 209)
		for _, str := range colDef.Tp.Elems {
			trace_util_0.Count(_preprocess_00000, 219)
			if strings.Contains(str, ",") {
				trace_util_0.Count(_preprocess_00000, 220)
				return types.ErrIllegalValueForType.GenWithStackByArgs(types.TypeStr(tp.Tp), str)
			}
		}
	case mysql.TypeNewDecimal:
		trace_util_0.Count(_preprocess_00000, 210)
		if tp.Decimal > mysql.MaxDecimalScale {
			trace_util_0.Count(_preprocess_00000, 221)
			return types.ErrTooBigScale.GenWithStackByArgs(tp.Decimal, colDef.Name.Name.O, mysql.MaxDecimalScale)
		}

		trace_util_0.Count(_preprocess_00000, 211)
		if tp.Flen > mysql.MaxDecimalWidth {
			trace_util_0.Count(_preprocess_00000, 222)
			return types.ErrTooBigPrecision.GenWithStackByArgs(tp.Flen, colDef.Name.Name.O, mysql.MaxDecimalWidth)
		}
	default:
		trace_util_0.Count(_preprocess_00000, 212)
		// TODO: Add more types.
	}
	trace_util_0.Count(_preprocess_00000, 198)
	return nil
}

// isDefaultValNowSymFunc checks whether defaul value is a NOW() builtin function.
func isDefaultValNowSymFunc(expr ast.ExprNode) bool {
	trace_util_0.Count(_preprocess_00000, 223)
	if funcCall, ok := expr.(*ast.FuncCallExpr); ok {
		trace_util_0.Count(_preprocess_00000, 225)
		// Default value NOW() is transformed to CURRENT_TIMESTAMP() in parser.
		if funcCall.FnName.L == ast.CurrentTimestamp {
			trace_util_0.Count(_preprocess_00000, 226)
			return true
		}
	}
	trace_util_0.Count(_preprocess_00000, 224)
	return false
}

func isInvalidDefaultValue(colDef *ast.ColumnDef) bool {
	trace_util_0.Count(_preprocess_00000, 227)
	tp := colDef.Tp
	// Check the last default value.
	for i := len(colDef.Options) - 1; i >= 0; i-- {
		trace_util_0.Count(_preprocess_00000, 229)
		columnOpt := colDef.Options[i]
		if columnOpt.Tp == ast.ColumnOptionDefaultValue {
			trace_util_0.Count(_preprocess_00000, 230)
			if !(tp.Tp == mysql.TypeTimestamp || tp.Tp == mysql.TypeDatetime) && isDefaultValNowSymFunc(columnOpt.Expr) {
				trace_util_0.Count(_preprocess_00000, 232)
				return true
			}
			trace_util_0.Count(_preprocess_00000, 231)
			break
		}
	}

	trace_util_0.Count(_preprocess_00000, 228)
	return false
}

// isIncorrectName checks if the identifier is incorrect.
// See https://dev.mysql.com/doc/refman/5.7/en/identifiers.html
func isIncorrectName(name string) bool {
	trace_util_0.Count(_preprocess_00000, 233)
	if len(name) == 0 {
		trace_util_0.Count(_preprocess_00000, 236)
		return true
	}
	trace_util_0.Count(_preprocess_00000, 234)
	if name[len(name)-1] == ' ' {
		trace_util_0.Count(_preprocess_00000, 237)
		return true
	}
	trace_util_0.Count(_preprocess_00000, 235)
	return false
}

// checkContainDotColumn checks field contains the table name.
// for example :create table t (c1.c2 int default null).
func (p *preprocessor) checkContainDotColumn(stmt *ast.CreateTableStmt) {
	trace_util_0.Count(_preprocess_00000, 238)
	tName := stmt.Table.Name.String()
	sName := stmt.Table.Schema.String()

	for _, colDef := range stmt.Cols {
		trace_util_0.Count(_preprocess_00000, 239)
		// check schema and table names.
		if colDef.Name.Schema.O != sName && len(colDef.Name.Schema.O) != 0 {
			trace_util_0.Count(_preprocess_00000, 241)
			p.err = ddl.ErrWrongDBName.GenWithStackByArgs(colDef.Name.Schema.O)
			return
		}
		trace_util_0.Count(_preprocess_00000, 240)
		if colDef.Name.Table.O != tName && len(colDef.Name.Table.O) != 0 {
			trace_util_0.Count(_preprocess_00000, 242)
			p.err = ddl.ErrWrongTableName.GenWithStackByArgs(colDef.Name.Table.O)
			return
		}
	}
}

func (p *preprocessor) handleTableName(tn *ast.TableName) {
	trace_util_0.Count(_preprocess_00000, 243)
	if tn.Schema.L == "" {
		trace_util_0.Count(_preprocess_00000, 247)
		currentDB := p.ctx.GetSessionVars().CurrentDB
		if currentDB == "" {
			trace_util_0.Count(_preprocess_00000, 249)
			p.err = errors.Trace(ErrNoDB)
			return
		}
		trace_util_0.Count(_preprocess_00000, 248)
		tn.Schema = model.NewCIStr(currentDB)
	}
	trace_util_0.Count(_preprocess_00000, 244)
	if p.flag&inCreateOrDropTable > 0 {
		trace_util_0.Count(_preprocess_00000, 250)
		// The table may not exist in create table or drop table statement.
		// Skip resolving the table to avoid error.
		return
	}
	trace_util_0.Count(_preprocess_00000, 245)
	table, err := p.is.TableByName(tn.Schema, tn.Name)
	if err != nil {
		trace_util_0.Count(_preprocess_00000, 251)
		p.err = err
		return
	}
	trace_util_0.Count(_preprocess_00000, 246)
	tn.TableInfo = table.Meta()
	dbInfo, _ := p.is.SchemaByName(tn.Schema)
	tn.DBInfo = dbInfo
}

func (p *preprocessor) resolveShowStmt(node *ast.ShowStmt) {
	trace_util_0.Count(_preprocess_00000, 252)
	if node.DBName == "" {
		trace_util_0.Count(_preprocess_00000, 254)
		if node.Table != nil && node.Table.Schema.L != "" {
			trace_util_0.Count(_preprocess_00000, 255)
			node.DBName = node.Table.Schema.O
		} else {
			trace_util_0.Count(_preprocess_00000, 256)
			{
				node.DBName = p.ctx.GetSessionVars().CurrentDB
			}
		}
	} else {
		trace_util_0.Count(_preprocess_00000, 257)
		if node.Table != nil && node.Table.Schema.L == "" {
			trace_util_0.Count(_preprocess_00000, 258)
			node.Table.Schema = model.NewCIStr(node.DBName)
		}
	}
	trace_util_0.Count(_preprocess_00000, 253)
	if node.User != nil && node.User.CurrentUser {
		trace_util_0.Count(_preprocess_00000, 259)
		// Fill the Username and Hostname with the current user.
		currentUser := p.ctx.GetSessionVars().User
		if currentUser != nil {
			trace_util_0.Count(_preprocess_00000, 260)
			node.User.Username = currentUser.Username
			node.User.Hostname = currentUser.Hostname
			node.User.AuthUsername = currentUser.AuthUsername
			node.User.AuthHostname = currentUser.AuthHostname
		}
	}
}

func (p *preprocessor) resolveAlterTableStmt(node *ast.AlterTableStmt) {
	trace_util_0.Count(_preprocess_00000, 261)
	for _, spec := range node.Specs {
		trace_util_0.Count(_preprocess_00000, 262)
		if spec.Tp == ast.AlterTableRenameTable {
			trace_util_0.Count(_preprocess_00000, 263)
			p.flag |= inCreateOrDropTable
			break
		}
	}
}

var _preprocess_00000 = "planner/core/preprocess.go"
