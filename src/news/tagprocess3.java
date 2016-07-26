package news;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

//把tag由字符串转为ID
public class tagprocess3 {
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
		PreparedStatement pstmt3=conn.prepareStatement("select userID, movieID, tag from finaltags");
		result=pstmt3.executeQuery();
		conn.setAutoCommit(false);
		PreparedStatement pstmt=conn.prepareStatement("insert into tagnames(userID,movieID,tagID,weight) values (?,?,?,?)");
		int count=0;
		while(result.next())
		{
			int userID=result.getInt(1);
			int movieID=result.getInt(2);
			String tag=result.getString(3);
			int tagID=tags.indexOf(tag);
			double weight=tagweight(tag,movieID,conn);
			System.out.println(weight);
			pstmt.setInt(1, userID);
			pstmt.setInt(2, movieID);
			pstmt.setInt(3,tagID);
			pstmt.setDouble(4, weight);
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

	private static double tagweight(String tag, int movieID,Connection conn ) throws SQLException {
		// TODO Auto-generated method stub
		int n=0;
		int N=0;
		PreparedStatement pstmt=conn.prepareStatement("select tag from finaltags where movieID=?");
		pstmt.setInt(1, movieID);
		ResultSet result=pstmt.executeQuery();
		while(result.next())
		{
			String tagName=result.getString(1).toLowerCase().trim();
			n++;
			if(tagName.equals(tag))
				N++;
		}
		result.close();
		return (N+0.0)/n;
	}

}
