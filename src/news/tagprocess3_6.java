package news;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

//把tag由字符串转为ID
public class tagprocess3_6 {
	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException
	{
		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/movielens", "root", "123456");
		PreparedStatement pstmt10=conn.prepareStatement("select distinct tagID from tagnamesF where rating!=0 and rating>avgrating");
		ResultSet result3=pstmt10.executeQuery();
		ArrayList<Integer> tagIDs1=new ArrayList<Integer>();
		while(result3.next())
		{
			tagIDs1.add(result3.getInt(1));
		}
		result3.close();
		pstmt10.close();
		pstmt10=conn.prepareStatement("select distinct tagID from tagnamesF where rating!=0 and rating<avgrating");
		result3=pstmt10.executeQuery();
		ArrayList<Integer> tagIDs2=new ArrayList<Integer>();
		while(result3.next())
		{
			tagIDs2.add(result3.getInt(1));
		}
		result3.close();
		pstmt10.close();
		PreparedStatement pstmt3=conn.prepareStatement("select userID, movieID, tagID from tagnames");
		ResultSet result=pstmt3.executeQuery();
		conn.setAutoCommit(false);
		PreparedStatement pstmt=conn.prepareStatement("insert into tagnamesFF(userID,movieID,tagID) values (?,?,?)");
		int count=0;
		while(result.next())
		{
			int userID=result.getInt(1);
			int movieID=result.getInt(2);
			int tagID=result.getInt(3);
			if(tagIDs1.contains(tagID)&&tagIDs2.contains(tagID))
			{
				pstmt.setInt(1, userID);
				pstmt.setInt(2, movieID);
				pstmt.setInt(3,tagID);
				pstmt.executeUpdate();
				count++;
			}
			if(count%1000==999)
			{
				conn.commit();
			}
		}
		conn.commit();
		conn.close();
	}

}
