package news;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;


public class tagprocess9_new {
	static int movie_ratings[][];
	static double tag_ratings[][];
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
		//tag-ratings
		tag_ratings=new double[userIDs.size()][tagIDs.size()];
		f.pstmt=f.conn.prepareStatement("select userID,tagID,rating from tagratings");
		result=f.pstmt.executeQuery();
		while(result.next())
		{
			int userID=result.getInt(1);
			int tagID=result.getInt(2);
			double rating=result.getDouble(3);
			int j=tagID;
			int i=userIDs.indexOf(userID);
			tag_ratings[i][j]=rating;
		}
		result.close();
		f.pstmt.close();
		//tag-weights
		f.conn.setAutoCommit(false);
		PreparedStatement pstmt=f.conn.prepareStatement("insert into newtagweights(movieID,tagID,weight) values (?,?,?)");
		int count=0;
		for(int i=0;i<movieIDs.size();i++)
		{
			for(int j=0;j<tagIDs.size();j++)
			{
				int movieID=movieIDs.get(i);
				int tagID=tagIDs.get(j);
				ArrayList<Integer> similar_users=new ArrayList<Integer>();
				ArrayList<Double> similar_user_ratings=new ArrayList<Double>();
				for(int t=0;t<userIDs.size();t++)
				{
					if(movie_ratings[t][i]!=0)
					{
						similar_users.add(t);
						similar_user_ratings.add(movie_ratings[t][i]/10.0);
					}
				}
				ArrayList<Double> tag_ratings2=new ArrayList<Double>();
				ArrayList<Double> movie_ratings2=new ArrayList<Double>();
				for(int k=0;k<similar_users.size();k++)
				{
					double r=tag_ratings[similar_users.get(k)][j];
					tag_ratings2.add(r);
					movie_ratings2.add(similar_user_ratings.get(k));
				}
				double similarity=pearson_similarity(tag_ratings2,movie_ratings2);
				double weight=Math.abs(similarity);
				if(weight!=0)
				{
					pstmt.setInt(1,movieID);
					pstmt.setInt(2,tagID);
					pstmt.setDouble(3,weight);
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
		pstmt.close();
		f.conn.close();
	}
	
	public static double pearson_similarity(ArrayList<Double> a,ArrayList<Double> b)
	{
		if(a.size()!=b.size())
		{
			System.out.println("Error! Fuction: pearson_similarity");
			System.exit(0);
		}
		else
		{
			double sum_a=0;
			double sum_b=0;
			double average_a=0;
			double average_b=0;
			int count=0;
			for(int i=0;i<a.size();i++)
			{
				
				if(a.get(i).doubleValue()!=0&&b.get(i).doubleValue()!=0)
				{
					sum_a+=a.get(i).doubleValue();
					count++;
					sum_b+=b.get(i).doubleValue();
				}
			}
			average_a=sum_a/count;
			average_b=sum_b/count;
			double sum1=0;
			double sum2=0;
			double sum3=0;
			for(int i=0;i<a.size();i++)
			{
				if(a.get(i).doubleValue()!=0&&b.get(i).doubleValue()!=0)
				{
					sum1+=(a.get(i).doubleValue()-average_a)*(b.get(i).doubleValue()-average_b);
					sum2+=Math.pow(a.get(i).doubleValue()-average_a, 2);
					sum3+=Math.pow(b.get(i).doubleValue()-average_b, 2);
				}
			}
			if(sum2==0&&sum3==0)
				return 1;
			else if(sum2==0||sum3==0)
				return 0;
			else
				return sum1/Math.sqrt(sum2*sum3);
		}
		return 0;
		
	}
}
