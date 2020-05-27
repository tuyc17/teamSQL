package cn.edu.thssdb.parser;


import cn.edu.thssdb.exception.KeyNotExistException;

public class mySQLvisitor extends SQLBaseVisitor<statement_data> {
    statement_data result = new statement_data();

    @Override
    public statement_data visitTable_query(SQLParser.Table_queryContext ctx) {
        int num1 = ctx.table_name().size();
        for (int i = 0; i < num1; i++) {
            result.table_names.add(ctx.table_name(i).getText());
        }
        if (ctx.multiple_condition()==null){
            return result;
        }
        String str1 = ctx.multiple_condition().condition().expression(0).getText();
        String str2 = ctx.multiple_condition().condition().expression(1).getText();
        EqualExpression temp = new EqualExpression();

        String[] k = str1.split("\\.");
        FullColumn temp2 = new FullColumn();
        if (k.length == 0) {
            //这种情况是*不考虑

        } else if (k.length == 1) {

            temp2.tableName = "NULL";
            temp2.column_name = k[0];
        } else if (k.length == 2) {
            temp2.tableName = k[0];
            temp2.column_name = k[1];
        }
        temp.column1 = temp2;

        String[] k2 = str2.split("\\.");
        FullColumn temp3 = new FullColumn();
        if (k2.length == 0) {
            //这种情况是*不考虑
        } else if (k2.length == 1) {

            temp3.tableName = "NULL";
            temp3.column_name = k[0];
        } else if (k2.length == 2) {
            temp3.tableName = k[0];
            temp3.column_name = k[1];
        }
        temp.column2 = temp3;
        result.equalexpressions.add(temp);
        return result;
    }

    @Override
    public statement_data visitCondition(SQLParser.ConditionContext ctx) {
        Condition temp = new Condition();
        temp.left = ctx.expression(0).getText();
        temp.right = ctx.expression(0).getText();
        temp.comparator = ctx.comparator().getText();
        result.conditions.add(temp);
        return result;
    }

    @Override
    public statement_data visitValue_entry(SQLParser.Value_entryContext ctx) {
        int num2 = ctx.literal_value().size();
        for (int i = 0; i < num2; i++) {
            result.value_entrys.add(ctx.literal_value().get(i).getText());
        }
        return result;
    }

    @Override
    public statement_data visitTable_constraint(SQLParser.Table_constraintContext ctx) {
        for (int i = 0; i < ctx.column_name().size(); i++) {
            int n = result.table.columns.size();
            for (int j = 0; j < n; j++) {
                if (ctx.column_name(i).getText().equals(result.table.columns.get(j).column_name)) {
                    result.table.columns.get(j).is_Primary_Key = true;
                }
            }
        }
        return result;
    }

    @Override
    public statement_data visitColumn_def(SQLParser.Column_defContext ctx) {
        Column temp = new Column();
        temp.type_name = ctx.type_name().getText();
        temp.column_name = ctx.column_name().getText();
        for (int i = 0; i < ctx.column_constraint().size(); i++) {
            if (ctx.column_constraint(i).getText().equals("notnull")) {
                temp.is_NotNull = true;
            }
            if (ctx.column_constraint(i).getText().equals("PrimaryKey")) {
                temp.is_Primary_Key = true;
            }
        }
        result.table.columns.add(temp);
        return result;
    }


    // 解析use
    @Override
    public statement_data visitUse_db_stmt(SQLParser.Use_db_stmtContext ctx) {
        result.kind = "use_database";
        result.database_name = ctx.database_name().getText();
        return result;
    }

    // 解析createdb
    @Override
    public statement_data visitCreate_db_stmt(SQLParser.Create_db_stmtContext ctx) {
        result.kind = "create_database";
        result.database_name = ctx.database_name().getText();
        return result;
    }

    // 解析dropdb
    @Override
    public statement_data visitDrop_db_stmt(SQLParser.Drop_db_stmtContext ctx) {
        result.kind = "drop_database";
        result.database_name = ctx.database_name().getText();
        return result;
    }

    // 解析createtable
    @Override
    public statement_data visitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx) {
        result.kind = "create_table";
        result.table.table_name = ctx.table_name().getText();
        for (int i = 0; i < ctx.column_def().size(); i++) {
            visit(ctx.column_def(i));
        }
        visit(ctx.table_constraint());
        return result;
    }

    // 解析droptable
    @Override
    public statement_data visitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx) {
        result.kind = "drop_table";
        result.table.table_name = ctx.table_name().getText();
        return result;
    }

    // 解析增insert
    @Override
    public statement_data visitInsert_stmt(SQLParser.Insert_stmtContext ctx) {
        result.kind = "insert";
        result.table_name = ctx.table_name().getText();
        int num1 = ctx.column_name().size();
        for (int i = 0; i < num1; i++) {
            result.column_names.add(ctx.column_name().get(i).getText());
        }
        visit(ctx.value_entry(0));
        return result;
    }

    // 解析删delete
    @Override
    public statement_data visitDelete_stmt(SQLParser.Delete_stmtContext ctx) {
        result.kind = "delete";
        result.table_name = ctx.table_name().getText();
        visit(ctx.multiple_condition());
        return result;
    }

    // 解析更新update
    @Override
    public statement_data visitUpdate_stmt(SQLParser.Update_stmtContext ctx) {
        result.kind = "update";
        result.table_name = ctx.table_name().getText();
        result.expression.left = ctx.column_name().getText();
        result.expression.right = ctx.expression().getText();
        result.expression.comparator = "=";
        visitChildren(ctx);
        return result;
    }

    // 解析查询select
    @Override
    public statement_data visitSelect_stmt(SQLParser.Select_stmtContext ctx) {
        int num1 = ctx.result_column().size();
        int num2 = ctx.table_query().size();
        result.kind="select";
        for (int i = 0; i < num1; i++) {
            String temp = ctx.result_column(i).getText();
            String[] k = temp.split("\\.");
            if (k.length == 0) {
                //这种情况是*不考虑
            } else if (k.length == 1) {
                FullColumn temp2 = new FullColumn();
                temp2.tableName = "NULL";
                temp2.column_name = k[0];
                result.FullColumns.add(temp2);
            } else if (k.length == 2) {
                FullColumn temp2 = new FullColumn();
                temp2.tableName = k[0];
                temp2.column_name = k[1];
                result.FullColumns.add(temp2);
            }
        }
        for (int i = 0; i < num2; i++) {
            visit(ctx.table_query(i));
        }
        if (ctx.multiple_condition()!=null){
            visit(ctx.multiple_condition());
        }
        return result;
    }
}
