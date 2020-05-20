package cn.edu.thssdb.parser;



public class mySQLvisitor extends SQLBaseVisitor<statement_data> {
    statement_data result = new statement_data();
    @Override
    public statement_data visitSql_stmt_list(SQLParser.Sql_stmt_listContext ctx) {
        System.out.print(ctx.getRuleContext());
        return result;
    }

}
