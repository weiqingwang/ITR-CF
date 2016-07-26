import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

//∂¡»ÎÀ˘”–tag
public class tagProcess {
	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException
	{
		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/movielens", "root", "123456");
		conn.setAutoCommit(false);
		PreparedStatement pstmt=conn.prepareStatement("insert into tags(userID,movieID,tag) values (?,?,?)");
		BufferedReader input=new BufferedReader(new FileReader("E:/movielens/tags.dat"));
		String str=input.readLine();
		int count=0;
		while(str!=null)
		{
			String[] ff=str.split("::");
			int userID=Integer.parseInt(ff[0]);
			int movieID=Integer.parseInt(ff[1]);
			String tag=ff[2].toLowerCase().trim();
			if(true)
			{
				pstmt.setInt(1, userID);
				pstmt.setInt(2, movieID);
				pstmt.setString(3,tag);
				pstmt.executeUpdate();
				count++;
			}
			if(count%10000==9999)
			{
				conn.commit();
			}
			str=input.readLine();
		}
		conn.commit();
		conn.close();
	}

}
