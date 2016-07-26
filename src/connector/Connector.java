package connector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class Connector {
	public Connection conn=null;
	
	public Connector() throws ClassNotFoundException, SQLException
	{
		conn = getConnection();
	}
	
	public static Connection getConnection() throws ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/movielens", "root", "123456");
		return conn;
	}
}
