package news;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class fillrating {
	static double movie_ratings[];
	static double tag_weights[][];
	static double tag_ratings[];
	static double fill_ratings[];
	static ArrayList<Integer> movieIDs;
	static ArrayList<Integer> userIDs;
	static ArrayList<Integer> tagIDs;
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
		//tag
		f.pstmt=f.conn.prepareStatement("select distinct tagID from tagNamesFM");
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
		f.pstmt.close();
		for(int i=0;i<userIDs.size();i++)
		{
			//user-movie rating
			movie_ratings=new double[movieIDs.size()];
			f.pstmt=f.conn.prepareStatement("select movieID,rating from finalratings where userID=?");
			f.pstmt.setInt(1, userIDs.get(i));
			result=f.pstmt.executeQuery();
			while(result.next())
			{
				int movieID=result.getInt(1);
				double rating=result.getDouble(2);
				int j=movieIDs.indexOf(movieID);
				movie_ratings[j]=rating;
			}
			result.close();
			f.pstmt.close();
			tag_ratings=new double[tagIDs.size()];
			f.pstmt=f.conn.prepareStatement("select tagID,rating from tagratings where userID=?");
			f.pstmt.setInt(1, userIDs.get(i));
			result=f.pstmt.executeQuery();
			while(result.next())
			{
				int tagID=result.getInt(1);
				double rating=result.getDouble(2);
				int j=tagIDs.indexOf(tagID);
				tag_ratings[j]=rating;
			}
			result.close();
			f.pstmt.close();
			f.conn.setAutoCommit(false);
			PreparedStatement pstmt=f.conn.prepareStatement("insert into fillratings(userID,movieID,rating) values (?,?,?)");
			for(int j=0;j<movieIDs.size();j++)
			{
				if(movie_ratings[j]==0)
				{
					double rating=0;
					for(int k=0;k<tagIDs.size();k++)
					{
						rating+=tag_ratings[k]*tag_weights[k][j];
					}
					pstmt.setInt(1, userIDs.get(i));
					pstmt.setInt(2, movieIDs.get(j));
					pstmt.setDouble(3, rating);
					pstmt.executeUpdate();
				}
			}
			f.conn.commit();
		}
		f.conn.close();
	}

}
