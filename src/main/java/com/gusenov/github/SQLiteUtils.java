package com.gusenov.github;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class SQLiteUtils {
    private static final String sharps = "#############################################";

    private static Logger logger;

    public static Logger getLogger() {
        return logger;
    }

    public static void setLogger(Logger logger) {
        SQLiteUtils.logger = logger;
    }

    /**
     * @see <a href="https://stackoverflow.com/a/30388953/2289640">Static initializer in Java</a>
     * @see <a href="https://stackoverflow.com/a/15758768/2289640">How to write logs in text file when using java.util.logging.Logger</a>
     * @see <a href="https://stackoverflow.com/questions/5817738/how-to-use-log-levels-in-java">How to use log levels in java</a>
     */
    static {
        logger = Logger.getLogger("sqlite-utils-java");
        FileHandler fh;
        try {
            // This block configure the logger with handler and formatter
            fh = new FileHandler("sqlite-utils.log");
            logger.addHandler(fh);
            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);
        } catch (Exception e) {
            logger.warning(String.format("\n\n%s\nException message: %s\n%s\nException message with class name: %s\n%s\n",
                    sharps, e.getMessage(), sharps, e.toString(), sharps));
            e.printStackTrace();
        }
    }

    /**
     * Подключиться к существующей БД или создать новую.
     *
     * @param fileName файл базы данных.
     *
     * @see <a href="http://www.sqlitetutorial.net/sqlite-java/create-database/">SQLite Java: Create a New SQLite Database</a>
     */
    public static Connection connectOrCreateNewDatabase(String fileName) {
        String url = "jdbc:sqlite:" + fileName;
        try {
            Connection conn = DriverManager.getConnection(url);
            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
                logger.info("The driver name is " + meta.getDriverName());
            }
            return conn;
        } catch (SQLException e) {
            logger.warning(String.format("\n\n%s\nException message: %s\n%s\nException message with class name: %s\n%s\n",
                    sharps, e.getMessage(), sharps, e.toString(), sharps));
            e.printStackTrace();
        }
        return null;
    }

    private interface ISortedMapLoopBody {
        void run(Object key, Object value, Integer index, Boolean isLast) throws Exception;
    }

    /**
     *
     * @param sortedMap
     * @param loopBody
     * @param <K>
     * @param <V>
     *
     * @see <a href="https://stackoverflow.com/a/4409134/2289640">How to make a Java Generic method static?</a>
     * @see <a href="https://stackoverflow.com/a/7427797/2289640">How to use SortedMap Interface in Java?</a>
     */
    private static <K, V> void loopThroughSortedMap(SortedMap<K, V> sortedMap, ISortedMapLoopBody loopBody) {
        Integer index = 0;
        for (Map.Entry<K, V> entry : sortedMap.entrySet()) {
            Boolean isLast = index == sortedMap.size() - 1;
            try {
                loopBody.run(entry.getKey(), entry.getValue(), index++, isLast);
            } catch (Exception e) {
                logger.warning(String.format("\n\n%s\nException message: %s\n%s\nException message with class name: %s\n%s\n",
                        sharps, e.getMessage(), sharps, e.toString(), sharps));
                e.printStackTrace();
            }

        }
    }

    /**
     * Создать новую таблицу в БД.
     *
     * @param conn      соединение с БД.
     * @param tableName наименование новой таблицы.
     * @param columns   наименования столбцов и их типы данных.
     *
     * @see <a href="http://www.sqlitetutorial.net/sqlite-java/create-table/">SQLite Java: Create a New Table</a>
     * @see <a href="https://www.javatpoint.com/StringBuilder-class">Java StringBuilder class</a>
     * @see <a href="https://dzone.com/articles/java-string-format-examples">Java String Format Examples</a>
     */
    public static void createNewTable(Connection conn, String tableName, SortedMap<String, String> columns) {
        StringBuilder sql = new StringBuilder(String.format("CREATE TABLE IF NOT EXISTS %s (\n", tableName));
        loopThroughSortedMap(columns, new ISortedMapLoopBody() {
            @Override
            public void run(Object key, Object value, Integer index, Boolean isLast) {
                String columnName = (String) key;
                String columnDatatype = (String) value;
                sql.append(String.format("	%s %s", columnName, columnDatatype));
                sql.append(isLast ? "\n" : ",\n");
            }
        });
        sql.append(");");
        logger.info(sql.toString());

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            logger.warning(String.format("\n\n%s\nException message: %s\n%s\nException message with class name: %s\n%s\n",
                    sharps, e.getMessage(), sharps, e.toString(), sharps));
            e.printStackTrace();
        }
    }

    private static void setValueForStatement(PreparedStatement pstmt, Integer index, Object value) throws SQLException {
        if (value instanceof String) pstmt.setString(index, (String) value);
        else if (value instanceof Double) pstmt.setDouble(index, (Double) value);
        else if (value instanceof Integer) pstmt.setInt(index, (Integer) value);
        else if (value instanceof Date) pstmt.setDate(index, (Date) value);
        else if (value instanceof BigDecimal) pstmt.setBigDecimal(index, (BigDecimal) value);
    }

    public static void insert(Connection conn, String tableName, SortedMap<String, Object> values) {
        StringBuilder sql = new StringBuilder(String.format("INSERT INTO %s(", tableName));
        StringBuilder sqlValues = new StringBuilder("VALUES(");
        loopThroughSortedMap(values, new ISortedMapLoopBody() {
            @Override
            public void run(Object key, Object value, Integer index, Boolean isLast) {
                sqlValues.append(isLast ? "?);" : "?, ");
                String columnName = (String) key;
                sql.append(columnName);
                sql.append(isLast ? ") " + sqlValues : ", ");
            }
        });
        logger.info(sql.toString());

        try (PreparedStatement pstmt = conn.prepareStatement(sql.toString())) {
            loopThroughSortedMap(values, new ISortedMapLoopBody() {
                @Override
                public void run(Object key, Object value, Integer index, Boolean isLast) throws Exception {
                    setValueForStatement(pstmt, index + 1, value);
                }
            });
            pstmt.executeUpdate();
        } catch (SQLException e) {
            logger.warning(String.format("\n\n%s\nException message: %s\n%s\nException message with class name: %s\n%s\n",
                    sharps, e.getMessage(), sharps, e.toString(), sharps));
            e.printStackTrace();
        }
    }

    /**
     * @see <a href="https://www.mathsisfun.com/equal-less-greater.html">Equal, Greater or Less Than</a>
     */
    public enum PredicateOperator {
        EQUALS, NOT_EQUAL_TO, GREATER_THAN, LESS_THAN, GREATER_THAN_OR_EQUAL_TO, LESS_THAN_OR_EQUAL_TO
    }

    /**
     * @see <a href="https://en.wikipedia.org/wiki/Fluent_interface#Java">Fluent interface</a>
     * @see <a href="https://en.wikipedia.org/wiki/Where_(SQL)">Where (SQL)</a>
     * @see <a href="https://stackoverflow.com/questions/20252727/is-not-an-enclosing-class-java">Is not an enclosing class Java</a>
     */
    public static class QueryCriteria {
        private StringBuilder sql = new StringBuilder("");

        private List<Object> values = new ArrayList<>();

        public List<Object> getValues() {
            return values;
        }

        public void setValues(List<Object> values) {
            this.values = values;
        }

        public QueryCriteria() {
        }

        private void add(String combineOperator, String column, String predicateOperator, Object value) {
            if (sql.toString().isEmpty()) {
                sql.append(String.format("(%s %s ?)", column, predicateOperator));
            } else {
                sql.append(String.format(" %s (%s %s ?)", combineOperator, column, predicateOperator));
            }
            values.add(value);
        }

        public QueryCriteria andEquals(String column, Object value) {
            add("AND", column, "=", value);
            return this;
        }

        public QueryCriteria andNotEqualTo(String column, Object value) {
            add("AND", column, "<>", value);
            return this;
        }

        public QueryCriteria andGreaterThan(String column, Object value) {
            add("AND", column, ">", value);
            return this;
        }

        public QueryCriteria andLessThan(String column, Object value) {
            add("AND", column, "<", value);
            return this;
        }

        public QueryCriteria andGreaterThanOrEqualTo(String column, Object value) {
            add("AND", column, ">=", value);
            return this;
        }

        public QueryCriteria andLessThanOrEqualTo(String column, Object value) {
            add("AND", column, "<=", value);
            return this;
        }

        public QueryCriteria orEquals(String column, Object value) {
            add("OR", column, "=", value);
            return this;
        }

        public QueryCriteria orNotEqualTo(String column, Object value) {
            add("OR", column, "<>", value);
            return this;
        }

        public QueryCriteria orGreaterThan(String column, Object value) {
            add("OR", column, ">", value);
            return this;
        }

        public QueryCriteria orLessThan(String column, Object value) {
            add("OR", column, "<", value);
            return this;
        }

        public QueryCriteria orGreaterThanOrEqualTo(String column, Object value) {
            add("OR", column, ">=", value);
            return this;
        }

        public QueryCriteria orLessThanOrEqualTo(String column, Object value) {
            add("OR", column, "<=", value);
            return this;
        }

        /**
         * @see <a href="https://stackoverflow.com/a/10734148/2289640">How to override toString() properly in Java?</a>
         */
        public String toString() {
            return sql.toString();
        }
    }

    public static void update(Connection conn, String tableName, SortedMap<String, Object> values, SortedMap<String, Object> criterions) {
        StringBuilder sql = new StringBuilder(String.format("UPDATE %s SET ", tableName));
        loopThroughSortedMap(values, new ISortedMapLoopBody() {
            @Override
            public void run(Object key, Object value, Integer index, Boolean isLast) {
                String columnName = (String) key;
                sql.append(String.format("%s = ?", columnName));
                sql.append(isLast ? "" : ", ");
            }
        });
        if (criterions.size() > 0) {
            sql.append(" WHERE ");
            loopThroughSortedMap(criterions, new ISortedMapLoopBody() {
                @Override
                public void run(Object key, Object value, Integer index, Boolean isLast) {
                    String columnName = (String) key;
                    sql.append(String.format("%s = ?", columnName));
                    sql.append(isLast ? "" : " AND ");
                }
            });
        }
        logger.info(sql.toString());

        try (PreparedStatement pstmt = conn.prepareStatement(sql.toString())) {
            loopThroughSortedMap(values, new ISortedMapLoopBody() {
                @Override
                public void run(Object key, Object value, Integer index, Boolean isLast) throws Exception {
                    setValueForStatement(pstmt, index + 1, value);
                }
            });

            if (criterions.size() > 0) {
                loopThroughSortedMap(criterions, new ISortedMapLoopBody() {
                    @Override
                    public void run(Object key, Object value, Integer index, Boolean isLast) throws Exception {
                        setValueForStatement(pstmt, values.size() + index + 1, value);
                    }
                });
            }

            pstmt.executeUpdate();
        } catch (SQLException e) {
            logger.warning(String.format("\n\n%s\nException message: %s\n%s\nException message with class name: %s\n%s\n",
                    sharps, e.getMessage(), sharps, e.toString(), sharps));
            e.printStackTrace();
        }
    }

    public interface IQueryResultLoopBody {
        void run(ResultSet rs) throws SQLException;
    }

    /**
     * Текстовый SQL-запрос строк из таблицы.
     *
     * @param sql      запрос к БД.
     * @param conn     соединение с БД.
     * @param loopBody функция для обработки результатов.
     *
     * @see <a href="http://www.sqlitetutorial.net/sqlite-java/select/">SQLite Java: Select Data</a>
     */
    public static void select(Connection conn, String sql, IQueryResultLoopBody loopBody) {
        logger.info(sql);
        try (Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)) {
            while (rs.next()) { // loop through the result set
                loopBody.run(rs);
            }
        } catch (SQLException e) {
            logger.warning(String.format("\n\n%s\nException message: %s\n%s\nException message with class name: %s\n%s\n",
                    sharps, e.getMessage(), sharps, e.toString(), sharps));
            e.printStackTrace();
        }
    }

    /**
     * @param conn
     * @param tableName
     * @param columns
     * @param criterions
     *
     * @see <a href="http://crunchify.com/how-to-iterate-through-java-list-4-way-to-iterate-through-loop/">How to iterate through Java List?</a>
     */
    public static void select(Connection conn, String tableName, List<String> columns, QueryCriteria criterions, IQueryResultLoopBody loopBody) {
        StringBuilder sql = new StringBuilder("SELECT ");
        if (columns.size() > 0) {
            for (int i = 0; i < columns.size(); i++) {
                if (i != columns.size() - 1) {
                    sql.append(String.format("%s, ", columns.get(i)));
                } else {
                    sql.append(String.format("%s FROM ", columns.get(i)));
                }
            }
        } else {
            sql.append("* FROM ");
        }
        sql.append(String.format("%s WHERE %s", tableName, criterions.toString()));
        logger.info(sql.toString());

        try (PreparedStatement pstmt  = conn.prepareStatement(sql.toString())) {
            // set the value
            for (int i = 1; i <= criterions.getValues().size(); i++) {
                setValueForStatement(pstmt, i, criterions.getValues().get(i - 1));
            }
            ResultSet rs  = pstmt.executeQuery();
            while (rs.next()) { // loop through the result set
                loopBody.run(rs);
            }
            rs.close();
        } catch (SQLException e) {
            logger.warning(String.format("\n\n%s\nException message: %s\n%s\nException message with class name: %s\n%s\n",
                    sharps, e.getMessage(), sharps, e.toString(), sharps));
            e.printStackTrace();
        }
    }

    /**
     * Удалить строку или строки.
     *
     * @param conn
     * @param tableName
     * @param criterions
     *
     * @see <a href="http://www.sqlitetutorial.net/sqlite-java/delete/">SQLite Java: Deleting Data</a>
     */
    public static void delete(Connection conn, String tableName, QueryCriteria criterions) {
        StringBuilder sql = new StringBuilder(String.format("DELETE FROM %s WHERE %s;", tableName, criterions));
        logger.info(sql.toString());

        try (PreparedStatement pstmt  = conn.prepareStatement(sql.toString())) {
            // set the corresponding param
            for (int i = 1; i <= criterions.getValues().size(); i++) {
                setValueForStatement(pstmt, i, criterions.getValues().get(i - 1));
            }
            pstmt.executeUpdate(); // execute the delete statement
        } catch (SQLException e) {
            logger.warning(String.format("\n\n%s\nException message: %s\n%s\nException message with class name: %s\n%s\n",
                    sharps, e.getMessage(), sharps, e.toString(), sharps));
            e.printStackTrace();
        }
    }
}
