package news;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class tagprocess3_1 {
	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException
	{
		ArrayList<String> tags=new ArrayList<String>();
		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/movielens", "root", "123456");
		PreparedStatement pstmt2=conn.prepareStatement("select distinct tag from finaltags");
		ResultSet result=pstmt2.executeQuery();
		while(result.next())
		{
			tags.add(result.getString(1));
		}
		result.close();
		pstmt2.close();
		FileWriter output=new FileWriter(new File("C:\\tags.txt"));
		for(int i=0;i<tags.size();i++)
		{
			output.write(i+"\t"+tags.get(i)+"\n");
		}
		output.close();
		conn.close();
	}

}
