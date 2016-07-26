package news;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;


public class tagprocess8 {
	static int movie_ratings[][];
	static double tag_weights[][];
	static ArrayList<Integer> movieIDs;
	static ArrayList<Integer> userIDs;
	static ArrayList<Integer> tagIDs;
	static ArrayList<Double> user_average_ratings;
	static ArrayList<Double> movie_average_ratings;
	public static void main(String[] args) throws ClassNotFoundException, SQLException
	{
		Algorithm f=new Algorithm();
		//movie
		f.pstmt=f.conn.prepareStatement("select distinct movieID from finalratings");
		ResultSet result=f.pstmt.executeQuery();
		movieIDs=new ArrayList<Integer>();
		while(result.next())
		{
			movieIDs.add(result.getInt(1));
		}
		result.close();
		f.pstmt.close();
		//user
		f.pstmt=f.conn.prepareStatement("select distinct userID from finalratings");
		result=f.pstmt.executeQuery();
		userIDs=new ArrayList<Integer>();
		while(result.next())
		{
			userIDs.add(result.getInt(1));
		}
		result.close();
		f.pstmt.close();
		//user-movie rating
		movie_ratings=new int[userIDs.size()][movieIDs.size()];
		f.pstmt=f.conn.prepareStatement("select userID,movieID,rating from finalratings");
		result=f.pstmt.executeQuery();
		while(result.next())
		{
			int userID=result.getInt(1);
			int movieID=result.getInt(2);
			double rating=result.getDouble(3);
			int i=userIDs.indexOf(userID);
			int j=movieIDs.indexOf(movieID);
			movie_ratings[i][j]=(int) (rating*10);
		}
		result.close();
		f.pstmt.close();
	/*	
		f.pstmt=f.conn.prepareStatement("select userID,movieID,rating from predictratings");
		result=f.pstmt.executeQuery();
		while(result.next())
		{
			int userID=result.getInt(1);
			int movieID=result.getInt(2);
			double rating=result.getDouble(3);
			int i=userIDs.indexOf(userID);
			int j=movieIDs.indexOf(movieID);
			movie_ratings[i][j]=(int) (rating*10);
		}
		result.close();
		f.pstmt.close();
		*/
		//user_average rating
		user_average_ratings=new ArrayList<Double>();
		for(int i=0;i<userIDs.size();i++)
		{
			int sum1=0;
			int sum2=0;
			for(int j=0;j<movieIDs.size();j++)
			{
				if(movie_ratings[i][j]!=0)
				{
					sum1+=movie_ratings[i][j];
					sum2+=10;
				}
			}
			user_average_ratings.add((sum1+0.0)/sum2);
		}
		//movie_average rating
		movie_average_ratings=new ArrayList<Double>();
		for(int i=0;i<movieIDs.size();i++)
		{
			double sum1=0;
			int sum2=0;
			for(int j=0;j<userIDs.size();j++)
			{
				if(movie_ratings[j][i]!=0)
				{
					sum1+=movie_ratings[j][i]/10.0-user_average_ratings.get(j);
					sum2+=1;
				}
			}
			movie_average_ratings.add(sum1/sum2);
		}
		//tag
		f.pstmt=f.conn.prepareStatement("select distinct tagID from tagNames");
		result=f.pstmt.executeQuery();
		tagIDs=new ArrayList<Integer>();
		while(result.next())
		{
			tagIDs.add(result.getInt(1));
		}
		System.out.println(tagIDs.size());
		result.close();
		f.pstmt.close();
		//tag-movie weights
		tag_weights=new double[tagIDs.size()][movieIDs.size()];
		f.pstmt=f.conn.prepareStatement("select tagID,movieID,weight from tagweights");
		result=f.pstmt.executeQuery();
		while(result.next())
		{
			int tagID=result.getInt(1);
			int movieID=result.getInt(2);
			double weight=result.getDouble(3);
			int i=tagID;
			int j=movieIDs.indexOf(movieID);
			if(j!=-1)
				tag_weights[i][j]=weight;
		}
		result.close();
		f.pstmt.close();/*
		f.pstmt=f.conn.prepareStatement("select tagID,movieID from tagNamesFM");
		result=f.pstmt.executeQuery();
		while(result.next())
		{
			int tagID=result.getInt(1);
			int movieID=result.getInt(2);
			int i=tagID;
			int j=movieIDs.indexOf(movieID);
			if(j!=-1)
				tag_weights[i][j]=-tag_weights[i][j];
		}
		result.close();
		f.pstmt.close();*/
		f.conn.setAutoCommit(false);
		PreparedStatement pstmt=f.conn.prepareStatement("insert into tagratings(userID,tagID,rating) values (?,?,?)");
		int count=0;
		for(int i=0;i<userIDs.size();i++)
		{
			for(int j=0;j<tagIDs.size();j++)
			{
				double rating=tag_rating(i,j);
				if(rating!=0)
				{
					pstmt.setInt(1, userIDs.get(i));
					pstmt.setInt(2, j);
					pstmt.setDouble(3, rating);
					pstmt.executeUpdate();
					count++;
					System.out.println(count);
				}
				if(count%10000==9999)
				{
					f.conn.commit();
				}
			}
		}
		f.conn.commit();
		f.conn.close();
	}

	public static double tag_rating(int i,int tagID) throws ClassNotFoundException, SQLException
	{
		int j=tagID;
		double tag_rating=0;
		double sum_weight=0;
		//double average_rating=user_average_ratings.get(i);
		for(int k=0;k<movieIDs.size();k++)
		{
			if(tag_weights[j][k]!=0)
			{
				if(movie_ratings[i][k]!=0)
				{
					tag_rating+=movie_ratings[i][k]*tag_weights[j][k]/10;
					sum_weight+=tag_weights[j][k];
				}
				/*else
				{
					tag_rating+=(movie_average_ratings.get(k)+average_rating)*tag_weights[j][k];
					sum_weight+=tag_weights[j][k];
				}
				*/
			}
		}
		if(tag_rating!=0)
			tag_rating=tag_rating/sum_weight;
		return tag_rating;
	}
}
