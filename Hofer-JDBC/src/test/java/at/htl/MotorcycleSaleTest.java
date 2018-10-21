package at.htl;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class MotorcycleSaleTest {
    public static final String DRIVER_STRING = "org.apache.derby.jdbc.ClientDriver";
    public static final String CONNECTION_STRING = "jdbc:derby://localhost:1527/db";
    public static final String USER = "app";
    public static final String PASSWORD = "app";
    private static Connection conn;

    @BeforeClass
    public static void initJdbc() {
        try {
            Class.forName(DRIVER_STRING);
            conn = DriverManager.getConnection(CONNECTION_STRING,USER,PASSWORD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Verbindung zu Datenbank nicht möglich: " + e.getMessage());
            System.exit(1);
        }

        try {
            //TABLE MotorcycleSale + INSERTS
            Statement stmt = conn.createStatement();
            String sql = "CREATE TABLE motorcyclesale (" +
                    "id INT CONSTRAINT motorcyclesale_pk PRIMARY KEY," +
                    "name VARCHAR(255) NOT NULL," +
                    "location VARCHAR(255) NOT NULL)";
            stmt.execute(sql);

            //TABLE MotorcycleType + INSERTS
            String sql2 = "CREATE TABLE motorcycletype (" +
                    "id INT CONSTRAINT motorcycletype_pk PRIMARY KEY," +
                    "name VARCHAR(255) NOT NULL)";
            stmt.execute(sql2);

            //TABLE Motorcycle + INSERTS
            String sql3 = "CREATE TABLE motorcycle (" +
                    "id INT CONSTRAINT sale_pk PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)," +
                    "horsepower INT NOT NULL," +
                    "cubiccentimeter INT NOT NULL," +
                    "colour VARCHAR(255) NOT NULL," +
                    "motorcyclesale_id INT NOT NULL CONSTRAINT fk_motorcyclesale_id references motorcyclesale(id)," +
                    "motorcycletype_id INT NOT NULL CONSTRAINT fk_motorcycletype_id references motorcycletype(id))";
            stmt.execute(sql3);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void teardownJdbc() {
        try {
            conn.createStatement().execute("DROP TABLE motorcycle");
            System.out.println("Tabelle MOTORCYCLE gelöscht");
        } catch (SQLException e) {
            System.out.println("Tabelle MOTORCYCLE konnte nicht gelöscht werden: " + e.getMessage());
        }

        try {
            conn.createStatement().execute("DROP TABLE motorcyclesale");
            System.out.println("Tabelle MOTORCYCLESALE gelöscht");
        } catch (SQLException e) {
            System.out.println("Tabelle MOTORCYCLESALE konnte nicht gelöscht werden: " + e.getMessage());
        }
        try {
            conn.createStatement().execute("DROP TABLE motorcycletype");
            System.out.println("Tabelle MOTORCYCLETYPE gelöscht");
        } catch (SQLException e) {
            System.out.println("Tabelle MOTORCYCLETYPE konnte nicht gelöscht werden: " + e.getMessage());
        }

        try {
            if(conn != null && !conn.isClosed()) {
                conn.close();
                System.out.println("Goodbye");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test() {}

    @Test
    public void motorcycleSaleTest() {
        int countInserts = 0;
        try {
            Statement stmt = conn.createStatement();
            String sql = "INSERT INTO motorcyclesale (id, name, location) VALUES (1, 'Franz motorcycles', 'Linz')";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO motorcyclesale (id, name, location) VALUES (2, 'Hannes motorcycles', 'Wels')";
            countInserts += stmt.executeUpdate(sql);

        } catch (SQLException e) {
            e.printStackTrace();
        }
        assertThat(countInserts, is(2));

        try {
            PreparedStatement pstmt = conn.prepareStatement("SELECT id, name, location FROM motorcyclesale");
            ResultSet rs = pstmt.executeQuery();
            rs.next();
            assertThat(rs.getString("NAME"), is("Franz motorcycles"));
            assertThat(rs.getString("LOCATION"), is("Linz"));
            rs.next();
            assertThat(rs.getString("NAME"), is("Hannes motorcycles"));
            assertThat(rs.getString("LOCATION"), is("Wels"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void motorcycleTypeTest() {
        int countInserts = 0;
        try {
            Statement stmt = conn.createStatement();
            String sql = "INSERT INTO motorcycletype (id, name) VALUES (1, 'Kawasaki Ninja')";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO motorcycletype (id, name) VALUES (2, 'KTM Duke')";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO motorcycletype (id, name) VALUES (3, 'Honda CBR')";
            countInserts += stmt.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        assertThat(countInserts, is(3));

        try {
            PreparedStatement pstmt = conn.prepareStatement("SELECT id, name FROM motorcycletype");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("NAME"), is("Kawasaki Ninja"));
            rs.next();
            assertThat(rs.getString("NAME"), is("KTM Duke"));
            rs.next();
            assertThat(rs.getString("NAME"), is("Honda CBR"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void motorcycleTest() {
        int countInserts = 0;
        try {
            Statement stmt = conn.createStatement();
            String sql = "INSERT INTO motorcyclesale (id, name, location) VALUES (1, 'Franz motorcycles', 'Linz')";
            stmt.executeUpdate(sql);
            sql = "INSERT INTO motorcyclesale (id, name, location) VALUES (2, 'Hannes motorcycles', 'Wels')";
            stmt.executeUpdate(sql);
            sql = "INSERT INTO motorcycletype (id, name) VALUES (1, 'Kawasaki Ninja')";
            stmt.executeUpdate(sql);
            sql = "INSERT INTO motorcycletype (id, name) VALUES (2, 'KTM Duke')";
            stmt.executeUpdate(sql);
            sql = "INSERT INTO motorcycletype (id, name) VALUES (3, 'Honda CBR')";
            stmt.executeUpdate(sql);
            sql = "INSERT INTO motorcycle(horsepower, cubiccentimeter, colour, motorcyclesale_id, motorcycletype_id) " +
                    "VALUES (140, 998, 'green', 1, 1)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO motorcycle(horsepower, cubiccentimeter, colour, motorcyclesale_id, motorcycletype_id) " +
                    "VALUES (120, 1290, 'orange', 1, 2)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO motorcycle(horsepower, cubiccentimeter, colour, motorcyclesale_id, motorcycletype_id) " +
                    "VALUES (130, 1000, 'red',  2, 3)";
            countInserts += stmt.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        assertThat(countInserts, is(3));

        try {
            PreparedStatement pstmt = conn.prepareStatement("SELECT horsepower, cubiccentimeter, colour FROM motorcycle");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getInt("HORSEPOWER"), is(140));
            assertThat(rs.getInt("CUBICCENTIMETER"), is(998));
            assertThat(rs.getString("COLOUR"), is("green"));
            rs.next();
            assertThat(rs.getInt("HORSEPOWER"), is(120));
            assertThat(rs.getInt("CUBICCENTIMETER"), is(1290));
            assertThat(rs.getString("COLOUR"), is("orange"));
            rs.next();
            assertThat(rs.getInt("HORSEPOWER"), is(130));
            assertThat(rs.getInt("CUBICCENTIMETER"), is(1000));
            assertThat(rs.getString("COLOUR"), is("red"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
