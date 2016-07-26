package news;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

//把tag由字符串转为ID
public class tagprocess3_7 {
	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException
	{
		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/movielens", "root", "123456");
		PreparedStatement pstmt10=conn.prepareStatement("select distinct tagID from tagnamesFF");
		ResultSet result3=pstmt10.executeQuery();
		ArrayList<Integer> tagIDs=new ArrayList<Integer>();
		while(result3.next())
		{
			tagIDs.add(result3.getInt(1));
		}
		result3.close();
		pstmt10.close();
		PreparedStatement pstmt3=conn.prepareStatement("select userID, movieID, tagID from tagnamesFF");
		ResultSet result=pstmt3.executeQuery();
		conn.setAutoCommit(false);
		PreparedStatement pstmt=conn.prepareStatement("insert into tagnamesFM(userID,movieID,tagID) values (?,?,?)");
		int count=0;
		while(result.next())
		{
			int userID=result.getInt(1);
			int movieID=result.getInt(2);
			int tagID=result.getInt(3);
			pstmt.setInt(1, userID);
			pstmt.setInt(2, movieID);
			pstmt.setInt(3,tagIDs.indexOf(tagID));
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
