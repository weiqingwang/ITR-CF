package news;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

//过滤用户，用户评过300部以上电影
public class ratingProcess2 {
	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException
	{
		ArrayList<Integer> movieIDs=new ArrayList<Integer>();
		ArrayList<Integer> userIDs=new ArrayList<Integer>();
		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/movies", "root", "123456");
		PreparedStatement pstmt2=conn.prepareStatement("select distinct movieID from tagnames");
		ResultSet result=pstmt2.executeQuery();
		while(result.next())
		{
			movieIDs.add(result.getInt(1));
		}
		PreparedStatement pstmt3=conn.prepareStatement("select distinct userID from tempratings group by userID having count(distinct movieID)>280");
		result=pstmt3.executeQuery();
		while(result.next())
		{
			userIDs.add(result.getInt(1));
		}
		conn.setAutoCommit(false);
		PreparedStatement pstmt=conn.prepareStatement("insert into newratings(userID,movieID,rating) values (?,?,?)");
		BufferedReader input=new BufferedReader(new FileReader("E:/movielens/ratings.dat"));
		String str=input.readLine();
		int count=0;
		while(str!=null)
		{
			String[] ff=str.split("::");
			int userID=Integer.parseInt(ff[0]);
			int movieID=Integer.parseInt(ff[1]);
			double rating=Double.parseDouble(ff[2]);
			if(movieIDs.contains(movieID)&&userIDs.contains(userID))
			{
				pstmt.setInt(1, userID);
				pstmt.setInt(2, movieID);
				pstmt.setDouble(3,rating);
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
