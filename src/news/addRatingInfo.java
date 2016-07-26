package news;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


public class addRatingInfo {
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException
	{
		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/movielens", "root", "123456");
		conn.setAutoCommit(false);
		PreparedStatement pstmt=conn.prepareStatement("insert into tagsR(userID,movieID,tag,rating) values (?,?,?,?)");
		PreparedStatement pstmt2=conn.prepareStatement("select rating from ratings where userID=? and movieID=?");
		BufferedReader input=new BufferedReader(new FileReader("E:/movielens/tags.dat"));
		String str=input.readLine();
		int count=0;
		while(str!=null)
		{
			String[] ff=str.split("::");
			int userID=Integer.parseInt(ff[0]);
			int movieID=Integer.parseInt(ff[1]);
			String tag=ff[2].toLowerCase().trim();
			if(tag.length()<30)
			{
				pstmt.setInt(1, userID);
				pstmt.setInt(2, movieID);
				pstmt.setString(3,tag);
				pstmt2.setInt(1, userID);
				pstmt2.setInt(2, movieID);
				conn.setAutoCommit(true);
				ResultSet result=pstmt2.executeQuery();
				if(result.next())
				{
					pstmt.setDouble(4, result.getDouble(1));
				}
				else
				{
					pstmt.setDouble(4, 0);
				}
				conn.setAutoCommit(false);
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
