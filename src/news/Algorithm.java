package news;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;


public class Algorithm {
	
	Connection conn=null;
	PreparedStatement  pstmt=null;
	PreparedStatement  pstmt2=null;
	
	public Algorithm() throws ClassNotFoundException, SQLException
	{
		conn = getConnection();
	}
	
	public void closeDatabase() throws SQLException
	{
		if(pstmt!=null)
			pstmt.close();
		if(conn!=null)
			conn.close();
	}
	
	public double tag_weight(int movieID, String tagName) throws SQLException, ClassNotFoundException
	{
		int n=0;
		int N=0;
		pstmt=conn.prepareStatement("select tagName from tagsCopy where movieID=?");
		pstmt.setInt(1, movieID);
		ResultSet result=pstmt.executeQuery();
		while(result.next())
		{
			String tag=result.getString(1).toLowerCase().trim();
			n++;
			if(tagName.equals(tag))
				N++;
		}
		result.close();
		return (N+0.0)/n;
	}
	
	public double movie_rating_exist(int userID,int movieID) throws SQLException
	{
		pstmt=conn.prepareStatement("select rating from ratings where userID=? and movieID=?");
		pstmt.setInt(1, userID);
		pstmt.setInt(2, movieID);
		ResultSet result=pstmt.executeQuery();
		if(result.next())
		{
			double rating=result.getDouble(1);
			result.close();
			pstmt.close();
			return rating;
		}
		else
		{
			result.close();
			pstmt.close();
			return -1;
		}
	}
	
	public double tag_rating(int userID,int tagID) throws SQLException, ClassNotFoundException
	{
		pstmt=conn.prepareStatement("select distinct movieID,weight from tagNames where tagID=?");
		pstmt.setInt(1, tagID);
		double sum1=0;//权重*评分的和
		double sum2=0;//权重和
		ResultSet result=pstmt.executeQuery();
		int movieID;
		double w;
		ArrayList<Integer> movieIDs=new ArrayList<Integer>();
		ArrayList<Double> weights=new ArrayList<Double>();
		while(result.next())
		{
			movieID=result.getInt(1);
			movieIDs.add(movieID);
			w=result.getDouble(2);
			weights.add(w);
		}
		result.close();
		pstmt.close();
		int count=0;
		for(int i=0;i<movieIDs.size();i++)
		{
			w=weights.get(i);
			double rating=movie_rating_exist(userID,movieIDs.get(i));
			if(rating!=-1)
			{
				count++;
				sum1+=rating*w;
				sum2+=w;
			}
		}
		if(sum2==0)
		{
			for(int i=0;i<movieIDs.size();i++)
			{
				
				if(movie_average_rating(movieIDs.get(i))!=0)
				{
					w=weights.get(i);
					sum1+=movie_average_rating(movieIDs.get(i))*w;
					sum2+=w;
				}
			}
		}
		return sum1/sum2;
	}
	
	private double movie_average_rating(int movieID) throws SQLException {
		// TODO Auto-generated method stub
		pstmt=conn.prepareStatement("select avg(rating) from ratings where movieID=?");
		pstmt.setInt(1, movieID);
		ResultSet result=pstmt.executeQuery();
		result.next();
		double rating=result.getDouble(1);
		result.close();
		pstmt.close();
		//System.out.println("avg:\t"+rating);
		return rating;
	}

	public double average_movie_rating(int userID) throws SQLException {
		pstmt=conn.prepareStatement("select avg(rating) from ratings where userID=?");
		pstmt.setInt(1, userID);
		ResultSet result=pstmt.executeQuery();
		result.next();
		double rating=result.getDouble(1);
		result.close();
		return rating;
		// TODO Auto-generated method stub
		
	}
	
	private static Connection getConnection() throws ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub
		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/movielens", "root", "123456");
		return conn;
	}
}
