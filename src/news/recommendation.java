package news;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;


public class recommendation {
	static int movie_ratings[][];
	static double tag_ratings[][];
	static ArrayList<Integer> movieIDs;
	static ArrayList<Integer> userIDs;
	static ArrayList<Integer> tagIDs;
	//static ArrayList<Integer> tag_movieIDs;
	//static ArrayList<Double> movie_average_ratings;
	static ArrayList<Double> user_average_ratings;
	static ArrayList<Double> user_average_tagratings;
	public static void main(String[] args) throws ClassNotFoundException, SQLException
	{
		Algorithm f=new Algorithm();
		//movie
		f.pstmt=f.conn.prepareStatement("select distinct movieID from tagnames");
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
		/*
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
		*/
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
		/*
		f.pstmt=f.conn.prepareStatement("select userID,tagID,rating from tagratings2");
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
		*/
		//user_average tag rating
		user_average_tagratings=new ArrayList<Double>();
		for(int i=0;i<userIDs.size();i++)
		{
			double sum1=0;
			int sum2=0;
			for(int j=0;j<tagIDs.size();j++)
			{
				if(tag_ratings[i][j]!=0)
				{
					sum1+=tag_ratings[i][j];
					sum2+=1;
				}
			}
			user_average_tagratings.add((sum1+0.0)/sum2);
		}
		f.pstmt=f.conn.prepareStatement("select userID,movieID,rating from predictratings");
		result=f.pstmt.executeQuery();
		ArrayList<Double> actual_ratings=new ArrayList<Double>();
		ArrayList<Double> predict_ratings=new ArrayList<Double>();
		f.pstmt=f.conn.prepareStatement("select distinct tagID from tagNames where movieID=?");
		int count=0;
		while(result.next())
		{
			int user=result.getInt(1);
			int movie=result.getInt(2);
			//System.out.println(result.getDouble(3));
			ArrayList<Integer> similar_users=new ArrayList<Integer>();
			ArrayList<Double> similar_user_ratings=new ArrayList<Double>();
			int id=movieIDs.indexOf(movie);
			int userid=userIDs.indexOf(user);
			for(int i=0;i<userIDs.size();i++)
			{
				if(movie_ratings[i][id]!=0)
				{
					similar_users.add(i);
					similar_user_ratings.add(movie_ratings[i][id]/10.0);
				}
			}
			if(similar_users.size()>0)
			{
				actual_ratings.add(result.getDouble(3));
			}
			else
			{
				continue;
			}
			f.pstmt.setInt(1, movie);
			ResultSet result2=f.pstmt.executeQuery();
			ArrayList<Integer> tags=new ArrayList<Integer>();
			while(result2.next())
			{
				tags.add(result2.getInt(1));
			}
			result2.close();
			ArrayList<Double> tag_ratings1=new ArrayList<Double>();
			double average_rating1;
			double summ=0;
			int count1=0;
			for(int j=0;j<tags.size();j++)
			{
				double rr;
				rr=tag_ratings[userid][tags.get(j)];
				summ+=rr;
				count1++;
				tag_ratings1.add(rr);
			}
			average_rating1=summ/count1;
			//average_rating1=user_average_ratings.get(userid);
			double sum1=0;
			double sum2=0;
			for(int k=0;k<similar_users.size();k++)
			{
				ArrayList<Double> tag_ratings2=new ArrayList<Double>();
				double average_rating2;
				double sum=0;
				int count2=0;
				for(int m=0;m<tags.size();m++)
				{
					double r=tag_ratings[similar_users.get(k)][tags.get(m)];
					tag_ratings2.add(r);
					sum+=r;
					count2++;
				}
				average_rating2=sum/count2;
				//average_rating2=user_average_ratings.get(similar_users.get(k));
				//out.println("average_rating2:\t"+average_rating2);
				//System.out.println(newtags.size());
				double similarity=pearson_similarity(userid,similar_users.get(k));
				//System.out.println(similar_user_ratings.get(k)+"\t"+(similar_user_ratings.get(k)-user_average_ratings.get(similar_users.get(k)))+"\t"+similarity);
				if(similarity>0)
				{
				sum1+=similarity*(similar_user_ratings.get(k)-average_rating2);
				sum2+=Math.abs(similarity);
				}
			}
			System.out.println(average_rating1);
			double predict_rating;
			if(sum2==0)
				predict_rating=average_rating1;
			else
				predict_rating=average_rating1+sum1/sum2;
			if(predict_rating>5)
				predict_rating=5;
			if(predict_rating<0.5)
				predict_rating=0.5;
			predict_ratings.add(predict_rating);
			count++;
			System.out.println(predict_rating);
			System.out.println(count+":\t"+(predict_rating-actual_ratings.get(predict_ratings.size()-1)));
		}
		result.close();
		f.pstmt.close();
		//f.pstmt2.close();
		System.out.println(MAE(actual_ratings,predict_ratings));
		System.out.println(RMSE(actual_ratings,predict_ratings));
		System.out.println(precision(actual_ratings,predict_ratings));
		f.closeDatabase();
	}
	
	private static double precision(ArrayList<Double> actual_ratings,
			ArrayList<Double> predict_ratings) {
		int num=actual_ratings.size();
		num=num/6;
		double count=0;
		for(int i=0;i<num/5;i++)
		{
			for(int j=30*i;j<30*i+30;j++)
			{
				int c=5;
				for(int k=30*i;k<30*i+30;k++)
				{
					if( predict_ratings.get(j)<=predict_ratings.get(k))
					{
						c--;
					}
				}
				if(c>=0)
				{
					if(actual_ratings.get(j)>user_average_ratings.get(i))
						count++;
				}
			}
		}
		return (count+0.0)/num;
	}

	private static double RMSE(ArrayList<Double> actual_ratings,
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
			for(int i=0;i<tagIDs.size();i++)
			{
				
				if(tag_ratings[userid][i]!=0&&tag_ratings[sim_userid][i]!=0)
				{
					sum_a+=tag_ratings[userid][i];
					count++;
					sum_b+=tag_ratings[sim_userid][i];
				}
			}
			average_a=sum_a/count;
			average_b=sum_b/count;
			for(int i=0;i<tagIDs.size();i++)
			{
				if(tag_ratings[userid][i]!=0&&tag_ratings[sim_userid][i]!=0)
				{
					double aa=tag_ratings[userid][i]-average_a;
					double bb=tag_ratings[sim_userid][i]-average_b;
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
	/*
	public static double pearson_similarity(int userid,int sim_userid)
	{
		double sum_a=0;
		double sum_b=0;
		double avg_a=0;
		double avg_b=0;
		int count=0;
		for(int i=0;i<tagIDs.size();i++)
		{
			if(tag_ratings[userid][i]!=0&&tag_ratings[sim_userid][i]!=0)
			{
				sum_a+=tag_ratings[userid][i];
				count++;
				sum_b+=tag_ratings[sim_userid][i];
			}
		}
		avg_a=sum_a/count;
		avg_b=sum_b/count;
		double sum1=0;
		double sum2=0;
		double sum3=0;
		for(int i=0;i<userIDs.size();i++)
		{
			if(tag_ratings[userid][i]!=0&&tag_ratings[sim_userid][i]!=0)
			{
				double aa=tag_ratings[userid][i]-avg_a;
				double bb=tag_ratings[sim_userid][i]-avg_b;
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
	*/
	/*
	public static double pearson_similarity(ArrayList<Double> a,ArrayList<Double> b,int userid,int sim_userid)
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
			average_a=user_average_tagratings.get(userid);
			average_b=user_average_tagratings.get(sim_userid);
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
	*/
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
			double avg_a=0;
			double avg_b=0;
			int count=0;
			for(int i=0;i<a.size();i++)
			{
				sum_a+=a.get(i);
				count++;
				sum_b+=b.get(i);
			}
			avg_a=sum_a/count;
			avg_b=sum_b/count;
			double sum1=0;
			double sum2=0;
			double sum3=0;
			for(int i=0;i<a.size();i++)
			{
				sum1+=(a.get(i)-avg_a)*(b.get(i)-avg_b);
				sum2+=Math.pow(a.get(i)-avg_a, 2);
				sum3+=Math.pow(b.get(i)-avg_b, 2);
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
