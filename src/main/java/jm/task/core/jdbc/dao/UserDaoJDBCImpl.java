package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;
import java.sql.*;
import java.util.*;

public class UserDaoJDBCImpl implements UserDao {
    private static long userCount = 0L;
    private final Connection connection = Util.getConnection();
    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() {
        // Check if table exists
        try {
            DatabaseMetaData meta = connection.getMetaData();
            ResultSet resultSet = meta.getTables(null, null, "USERS", new String[] {"TABLE"});
            if (resultSet.next()) {
                return;
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        String SQL = "CREATE TABLE Users " +
                "(id BIGINT, " +
                " Name VARCHAR(255), " +
                " LastName VARCHAR(255), " +
                " Age TINYINT, " +
                " PRIMARY KEY ( id ))";
        try {
            Statement statement = connection.createStatement();
            statement.execute(SQL);
            connection.commit();

        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException re) {
                re.printStackTrace();
            }
            e.printStackTrace();
        }

    }

    public void dropUsersTable() {
        //Check if table already doesn't exist
        try {
            DatabaseMetaData meta = connection.getMetaData();
            ResultSet resultSet = meta.getTables(null, null, "USERS", new String[] {"TABLE"});
            if (!resultSet.next()) {
                return;
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        String SQL = "DROP TABLE Users";
        try {
            Statement statement = connection.createStatement();
            statement.execute(SQL);
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException re) {
                re.printStackTrace();
            }
            e.printStackTrace();
        }
        userCount = 0L;
    }

    public void saveUser(String name, String lastName, byte age) {

        try {
            PreparedStatement pstmnt = connection.prepareStatement("INSERT INTO Users VALUES(?,?,?,?)");

            pstmnt.setLong(1, ++userCount);
            pstmnt.setString(2, name);
            pstmnt.setString(3, lastName);
            pstmnt.setByte(4, age);

            pstmnt.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException re) {
                re.printStackTrace();
            }
            e.printStackTrace();
        }
    }

    public void removeUserById(long id) {
        try {
            PreparedStatement pstmnt = connection.prepareStatement("DELETE FROM Users WHERE id=?");

            pstmnt.setLong(1, id);

            pstmnt.executeUpdate();
            connection.commit();

        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException re) {
                re.printStackTrace();
            }
            e.printStackTrace();
        }
    }

    public List<User> getAllUsers() {
        List<User> listOfUsers = new ArrayList<>();

        try {
            Statement statement = connection.createStatement();
            String SQL = "SELECT * FROM Users";
            ResultSet resultSet = statement.executeQuery(SQL);

            while (resultSet.next()) {
                User user = new User();

                user.setId(resultSet.getLong("id"));
                user.setName(resultSet.getString("Name"));
                user.setLastName(resultSet.getString("LastName"));
                user.setAge(resultSet.getByte("age"));

                listOfUsers.add(user);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return listOfUsers;
    }

    public void cleanUsersTable() {
        try {
            Statement statement = connection.createStatement();
            String SQL = "TRUNCATE TABLE Users";
            statement.executeUpdate(SQL);
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException re) {
                re.printStackTrace();
            }
            e.printStackTrace();
        }
        userCount = 0L;
    }
}
