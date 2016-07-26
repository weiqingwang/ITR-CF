package news;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class user_based_new {
	
	static int userNum;
	static int movieNum;
	static int ratingNum;
	static int tagNum;
	static int movie_ratings[][];
	static double tag_weights[][];
	static double tag_ratings[][];
	static ArrayList<Double> user_tag_averageR;
	static ArrayList<Double> movie_average_ratings;
	static ArrayList<Double> user_average_ratings;
	static ArrayList<Integer> movieIDs;
	static ArrayList<Integer> userIDs;
	
	public static double[] process() throws ClassNotFoundException, SQLException
	{
		double[] result_new=new double[5];
	Algorithm f=new Algorithm();
	//movie
	f.pstmt=f.conn.prepareStatement("select distinct movieID from tagnamestarget");
	ResultSet result=f.pstmt.executeQuery();
	movieIDs=new ArrayList<Integer>();
	while(result.next())
	{
		movieIDs.add(result.getInt(1));
	}
	movieNum=movieIDs.size();
	System.out.println("movie number:\t"+movieNum);
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
	userNum=userIDs.size();
	System.out.println("user number:\t"+userNum);
	result.close();
	f.pstmt.close();
	//user-movie rating
	movie_ratings=new int[userNum][movieNum];
	f.pstmt=f.conn.prepareStatement("select userID,movieID,rating from finalratings");
	result=f.pstmt.executeQuery();
	ratingNum=0;
	while(result.next())
	{
		int userID=result.getInt(1);
		int movieID=result.getInt(2);
		double rating=result.getDouble(3);
		int i=userIDs.indexOf(userID);
		int j=movieIDs.indexOf(movieID);
		movie_ratings[i][j]=(int) (rating*10);
		ratingNum++;
	}
	System.out.println("rating number:\t"+ratingNum);
	result.close();
	f.pstmt.close();
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
	f.pstmt=f.conn.prepareStatement("select userID,movieID,rating from predictratings");
	result=f.pstmt.executeQuery();
	ArrayList<Double> actual_ratings=new ArrayList<Double>();
	ArrayList<Double> predict_ratings=new ArrayList<Double>();
	int count=0;
	while(result.next())
	{
		int user=result.getInt(1);
		int movie=result.getInt(2);
		double rating=result.getDouble(3);
		//out.println("rating:"+rating);
		int movieid=movieIDs.indexOf(movie);
		int userid=userIDs.indexOf(user);
		ArrayList<Integer> similar_users=new ArrayList<Integer>();
		for(int i=0;i<userNum;i++)
		{
			if(movie_ratings[i][movieid]!=0&&i!=userid)
			{
				similar_users.add(i);
			}
		}
		double sum1=0;
		double sum2=0;
		for(int k=0;k<similar_users.size();k++)
		{
			int sim_userid=similar_users.get(k);
			//out.println("average_rating2:\t"+average_rating2);
			double similarity=pearson_similarity(userid,sim_userid);
			//similarity=1;
			if(true)
			{
				sum1+=similarity*(movie_ratings[sim_userid][movieid]/10.0-user_average_ratings.get(sim_userid));
				sum2+=Math.abs(similarity);
			}
		}
		//out.println(user_average_ratings.get(userid)+"\t"+rating);
		if(sum2==0)
			continue;
		double predict_rating=user_average_ratings.get(userid)+sum1/sum2;
		if(predict_rating>5)
			predict_rating=5;
		if(predict_rating<0.5)
			predict_rating=0.5;
		predict_ratings.add(predict_rating);
		actual_ratings.add(rating);
		count++;
		System.out.println(count+"\t"+(predict_rating-rating));
	}
	result.close();
	f.pstmt.close();
	result_new[0]=MAE(actual_ratings,predict_ratings);
	result_new[1]=RMSE(actual_ratings,predict_ratings);
	System.out.println(result_new[0]);
	System.out.println(result_new[1]);
//	System.out.println(precision(actual_ratings,predict_ratings));
	f.closeDatabase();
	return result_new;
}
	
public static double RMSE(ArrayList<Double> actual_ratings,
		ArrayList<Double> predict_ratings) {
	int n=actual_ratings.size();
	double sum=0;
	for(int i=0;i<actual_ratings.size();i++)
	{
		sum+=Math.pow(actual_ratings.get(i)-predict_ratings.get(i),2);
	}
	return sum/(n-1);
}

public static double MAE(ArrayList<Double> actual_ratings, ArrayList<Double> predict_ratings)
{
	System.out.println(actual_ratings.size());
	double sum=0;
	for(int i=0;i<actual_ratings.size();i++)
	{
		sum+=Math.abs(actual_ratings.get(i)-predict_ratings.get(i));
	}
	return sum/actual_ratings.size();
}

public static double pearson_similarity(int userid,int sim_userid)
{
		double sum1=0;
		double sum2=0;
		double sum3=0;
		double sum_a=0;
		double sum_b=0;
		double average_a=0;
		double average_b=0;
		int count=0;
		for(int i=0;i<movieNum;i++)
		{
			
			if(movie_ratings[userid][i]!=0&&movie_ratings[sim_userid][i]!=0)
			{
				sum_a+=movie_ratings[userid][i];
				count++;
				sum_b+=movie_ratings[sim_userid][i];
			}
		}
		average_a=sum_a/count;
		average_b=sum_b/count;
		for(int i=0;i<movieNum;i++)
		{
			if(movie_ratings[userid][i]!=0&&movie_ratings[sim_userid][i]!=0)
			{
				double aa=movie_ratings[userid][i]-average_a;
				double bb=movie_ratings[sim_userid][i]-average_b;
				sum1+=aa*bb;
				sum2+=aa*aa;
				sum3+=bb*bb;
			}
		}
		if(sum2==0&&sum3==0)
			return 1;
		else if(sum2==0||sum3==0)
			return 0;
		else
			return sum1/Math.sqrt(sum2*sum3);
}
}