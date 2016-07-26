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

//电影要求有15个以上tag
public class tagProcess2 {
	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException
	{
		ArrayList<String> tags=new ArrayList<String>();
		ArrayList<Integer> movieIDs=new ArrayList<Integer>();
		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/movielens", "root", "123456");
		PreparedStatement pstmt2=conn.prepareStatement("select distinct tag from tags group by tag having count(distinct userID)>2 and count(distinct movieID)>5");
		ResultSet result=pstmt2.executeQuery();
		while(result.next())
		{
			tags.add(result.getString(1));
		}
		PreparedStatement pstmt3=conn.prepareStatement("select distinct movieID from newtags group by movieID having count(distinct tag)>15");
		result=pstmt3.executeQuery();
		while(result.next())
		{
			movieIDs.add(result.getInt(1));
		}
		conn.setAutoCommit(false);
		PreparedStatement pstmt=conn.prepareStatement("insert into finaltags(userID,movieID,tag) values (?,?,?)");
		BufferedReader input=new BufferedReader(new FileReader("E:/movielens/tags.dat"));
		String str=input.readLine();
		int count=0;
		while(str!=null)
		{
			String[] ff=str.split("::");
			int userID=Integer.parseInt(ff[0]);
			int movieID=Integer.parseInt(ff[1]);
			String tag=ff[2].toLowerCase().trim();
			if(tags.contains(tag)&&movieIDs.contains(movieID))
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
