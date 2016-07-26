package news;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class tagprocess3_5 {
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
		PreparedStatement pstmt3=conn.prepareStatement("select userID, movieID, tagID from tagnames");
		result=pstmt3.executeQuery();
		conn.setAutoCommit(false);
		PreparedStatement pstmt=conn.prepareStatement("insert into tagnamesF(userID,movieID,tagID,rating,avgrating) values (?,?,?,?,?)");
		int count=0;
		while(result.next())
		{
			int userID=result.getInt(1);
			int movieID=result.getInt(2);
			int tagID=result.getInt(3);
			pstmt.setInt(1, userID);
			pstmt.setInt(2, movieID);
			pstmt.setInt(3,tagID);
			PreparedStatement pstmt10=conn.prepareStatement("select rating from ratings where userID=? and movieID=?");
			pstmt10.setInt(1, userID);
			pstmt10.setInt(2, movieID);
			ResultSet result3=pstmt10.executeQuery();
			double rating;
			if(result3.next())
			{
				rating=result3.getDouble(1);
			}
			else
			{
				rating=0;
			}
			result3.close();
			pstmt10.close();
			pstmt10=conn.prepareStatement("select avg(rating) from ratings where userID=?");
			pstmt10.setInt(1, userID);
			result3=pstmt10.executeQuery();
			double avgrating;
			if(result3.next())
			{
				avgrating=result3.getDouble(1);
			}
			else
			{
				avgrating=0;
			}
			result3.close();
			pstmt10.close();
			pstmt.setDouble(4, rating);
			pstmt.setDouble(5, avgrating);
			pstmt.executeUpdate();
			count++;
			if(count%1000==999)
			{
				conn.commit();
			}
		}
		conn.commit();
		conn.close();
	}

}
