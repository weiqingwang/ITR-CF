package news;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;
//划分训练集和测试集，每个用户随机挑30部电影作为测试集
public class ratingProcess3 {
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException
	{
		ArrayList<Integer> userIDs=new ArrayList<Integer>();
		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/movielens", "root", "123456");
		PreparedStatement pstmt2=conn.prepareStatement("select distinct userID from newratings");
		ResultSet result=pstmt2.executeQuery();
		while(result.next())
		{
			userIDs.add(result.getInt(1));
		}
		PreparedStatement pstmt=conn.prepareStatement("select movieID, rating from newratings where userID= ?");
		PreparedStatement pstmt5=conn.prepareStatement("select count(*) from newratings where userID= ?");
		PreparedStatement pstmt3=conn.prepareStatement("insert into finalratings(userID,movieID,rating) values (?,?,?)");
		PreparedStatement pstmt4=conn.prepareStatement("insert into predictratings(userID,movieID,rating) values (?,?,?)");
		for(int i=0;i<userIDs.size();i++)
		{
			conn.setAutoCommit(true);
			int userID=userIDs.get(i);
			pstmt5.setInt(1, userID);
			result=pstmt5.executeQuery();
			result.next();
			int total=result.getInt(1);
			ArrayList<Integer> randomNums=new ArrayList<Integer>();
			randomNums=random(total,30);
			pstmt.setInt(1,userID);
			result=pstmt.executeQuery();
			conn.setAutoCommit(false);
			int count=0;
			while(result.next())
			{
				count++;
				int movieID=result.getInt(1);
				double rating=result.getDouble(2);
				if(randomNums.contains(count))
				{
					pstmt4.setInt(1, userID);
					pstmt4.setInt(2, movieID);
					pstmt4.setDouble(3,rating);
					pstmt4.executeUpdate();
				}
				else
				{
					pstmt3.setInt(1, userID);
					pstmt3.setInt(2, movieID);
					pstmt3.setDouble(3,rating);
					pstmt3.executeUpdate();
				}
			}
			conn.commit();
		}
		conn.close();
	}

	private static ArrayList<Integer> random(int total, int num) {
		// TODO Auto-generated method stub
		int[] seed=new int[total];
		ArrayList<Integer> ranArr=new ArrayList<Integer>();
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
