package news;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;

//将ratingstemp中的用户的评分稀疏为原来的百分之十左右
//方法是随机的选择一个用户中的十分之一的评分插入表ratingstemp1表中。
public class RatingProcess2_new {
	private static double xishudu=0.5;
	public static void process() throws IOException, ClassNotFoundException, SQLException
	{
		ArrayList<Integer> userIDs=new ArrayList<Integer>();
		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/movies", "root", "123456");
		PreparedStatement pstmt=conn.prepareStatement("select distinct userID from ratingstemp");
		ResultSet result=pstmt.executeQuery();
		while(result.next())
		{
			userIDs.add(result.getInt(1));
		}
		result.close();
		pstmt.close();
		pstmt=conn.prepareStatement("delete from finalratings");
		pstmt.execute();
		pstmt.close();
		pstmt=conn.prepareStatement("delete from predictratings");
		pstmt.execute();
		pstmt.close();
		PreparedStatement pstmt3=conn.prepareStatement("insert into finalratings(userID,movieID,rating) values (?,?,?)");
		PreparedStatement pstmt2=conn.prepareStatement("select count(*) from ratingstemp where userID= ?");
		pstmt=conn.prepareStatement("insert into finalratings(userID,movieID,rating) values (?,?,?)");
		PreparedStatement pstmt1=conn.prepareStatement("select movieID,rating from ratingstemp where userID=?");
		for(int i=0;i<userIDs.size();i++)
		{
			conn.setAutoCommit(true);
			int userID=userIDs.get(i);
			pstmt2.setInt(1, userID);
			result=pstmt2.executeQuery();
			result.next();
			int total=result.getInt(1);
			result.close();
			ArrayList<Integer> randomNums=new ArrayList<Integer>();
			randomNums=random(total);
			pstmt1.setInt(1,userID);
			result=pstmt1.executeQuery();
			conn.setAutoCommit(false);
			int count=0;
			while(result.next())
			{
				count++;
				int movieID=result.getInt(1);
				double rating=result.getDouble(2);
				if(randomNums.contains(count))
				{
					pstmt.setInt(1, userID);
					pstmt.setInt(2, movieID);
					pstmt.setDouble(3,rating);
					pstmt.executeUpdate();
					pstmt3.setInt(1, userID);
					pstmt3.setInt(2, movieID);
					pstmt3.setDouble(3,rating);
					pstmt3.executeUpdate();
				}
			}
			result.close();
			conn.commit();
		}
		result.close();
		pstmt.close();
		pstmt1.close();
		pstmt2.close();
		pstmt3.close();	
		conn.close();
	}
	
	private static ArrayList<Integer> random(int total) {
		// TODO Auto-generated method stub
		int[] seed=new int[total];
		ArrayList<Integer> ranArr=new ArrayList<Integer>();
		int num=(int)(total*xishudu);
		for(int i=1;i<=total;i++)
		{
			seed[i-1]=i;
		}
		Random ran = new Random();
		for (int i = 0; i <num; i++) 
		{
			int j = ran.nextInt(seed.length - i);
			ranArr.add(seed[j]);
			seed[j] = seed[seed.length - 1 - i];
		}
		return ranArr;
	}
}
