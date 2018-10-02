package com.gusenov.github;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class SQLiteUtilsTest {
    /**
     * @see <a href="https://stackoverflow.com/a/531390/2289640">JUnit before and test</a>
     * @see <a href="https://stackoverflow.com/a/45575420/2289640">Java better way to delete file if exists</a>
     */
    @Before
    public void clear() {
        File db = new File("tests.db");
        if(db.exists() && !db.isDirectory()) {
            boolean result = db.delete();
        }

        File log = new File("sqlite-utils.log");
        if(log.exists() && !log.isDirectory()) {
            boolean result = log.delete();
        }
    }

    /**
     * @see <a href="https://stackoverflow.com/a/6802502/2289640">How to directly initialize a HashMap (in a literal way)? [duplicate]</a>
     * @see <a href="https://stackoverflow.com/a/1005083/2289640">Initialization of an ArrayList in one line</a>
     */
    @Test
    public void test() {
        Connection conn = SQLiteUtils.connectOrCreateNewDatabase("tests.db");




        {
            SortedMap<String, String> columns = new TreeMap<String, String>();
            columns.put("id", "integer PRIMARY KEY");
            columns.put("name", "text NOT NULL");
            columns.put("capacity", "real");
            SQLiteUtils.createNewTable(conn, "warehouses", columns);
        }




        {
            SortedMap<String, Object> values = new TreeMap<String, Object>();

            values.put("name", "Raw Materials");
            values.put("capacity", 3000);
            SQLiteUtils.insert(conn, "warehouses", values);

            values.put("name", "Semifinished Goods");
            values.put("capacity", 4000);
            SQLiteUtils.insert(conn, "warehouses", values);

            values.put("name", "Finished Goods");
            values.put("capacity", 5000);
            SQLiteUtils.insert(conn, "warehouses", values);
        }




        {
            String sql = "SELECT id, name, capacity FROM warehouses;";

            SQLiteUtils.select(conn, sql, new SQLiteUtils.IQueryResultLoopBody() {
                @Override
                public void run(ResultSet rs) throws SQLException {
                    assertNotNull(rs);
                    Integer id = rs.getInt("id");
                    String name = rs.getString("name");
                    Double capacity = rs.getDouble("capacity");
                    System.out.println(id + "\t" + name + "\t" + capacity);
                }
            });
        }




        {
            SortedMap<String, Object> values = new TreeMap<String, Object>();
            values.put("name", "Finished Products");
            values.put("capacity", 5500);
            SortedMap<String, Object> criterions = new TreeMap<String, Object>();
            criterions.put("id", 3);
            SQLiteUtils.update(conn, "warehouses", values, criterions);
        }




        {
            ArrayList<String> columns = new ArrayList<String>();
            columns.add("id");
            columns.add("name");
            columns.add("capacity");
            SQLiteUtils.QueryCriteria criterions = new SQLiteUtils.QueryCriteria();
            criterions.andGreaterThan("capacity", 3600);
            SQLiteUtils.select(conn, "warehouses", columns, criterions, new SQLiteUtils.IQueryResultLoopBody() {
                @Override
                public void run(ResultSet rs) throws SQLException {
                    assertNotNull(rs);
                    Integer id = rs.getInt("id");
                    String name = rs.getString("name");
                    Double capacity = rs.getDouble("capacity");
                    System.out.println(id + "\t" + name + "\t" + capacity);
                }
            });
        }




        {
            SQLiteUtils.QueryCriteria criterions = new SQLiteUtils.QueryCriteria();
            criterions.andEquals("id", 3);
            SQLiteUtils.delete(conn, "warehouses", criterions);
        }
    }
}