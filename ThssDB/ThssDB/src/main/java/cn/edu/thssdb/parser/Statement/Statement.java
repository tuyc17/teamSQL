package cn.edu.thssdb.parser.Statement;

public abstract class Statement {
    public enum Type {
        CREATE_DATABASE,
        DROP_DATABASE,
        USE,
        CREATE_TABLE,
        DROP_TABLE,
        SHOW_META,
        INSERT,
        DELETE,
        UPDATE,
        SELECT
    }

    public abstract Type get_type();
}
