package news;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;


public class CopyOftagprocess8 {
	static double tag_ratings[][];
	static double tag_distributions[][];
	static ArrayList<Integer> userIDs;
	static ArrayList<Integer> tagIDs;
	public static void main(String[] args) throws ClassNotFoundException, SQLException
	{
		Algorithm f=new Algorithm();
		//tag
		f.pstmt=f.conn.prepareStatement("select distinct tagID from tagNames");
		ResultSet result=f.pstmt.executeQuery();
		tagIDs=new ArrayList<Integer>();
		while(result.next())
		{
			tagIDs.add(result.getInt(1));
		}
		System.out.println(tagIDs.size());
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
		//user-tag rating
		tag_ratings=new double[userIDs.size()][tagIDs.size()];
		f.pstmt=f.conn.prepareStatement("select userID,tagID,rating from tagratings");
		result=f.pstmt.executeQuery();
		while(result.next())
		{
			int userID=result.getInt(1);
			int tagID=result.getInt(2);
			double rating=result.getDouble(3);
			int i=userIDs.indexOf(userID);
			int j=tagID;
			tag_ratings[i][j]=rating;
		}
		result.close();
		f.pstmt.close();
		//tag-distributions
		tag_distributions=new double[tagIDs.size()][tagIDs.size()];
		f.pstmt=f.conn.prepareStatement("select tagID1,tagID2,distribution from distributions");
		result=f.pstmt.executeQuery();
		while(result.next())
		{
			int tagID1=result.getInt(1);
			int tagID2=result.getInt(2);
			double distribution=result.getDouble(3);
			tag_distributions[tagID1][tagID2]=distribution;
		}
		result.close();
		f.pstmt.close();
		f.conn.setAutoCommit(false);
		PreparedStatement pstmt=f.conn.prepareStatement("insert into newtagratings(userid,tagID,rating) values (?,?,?)");
		int count=0;
		for(int i=0;i<userIDs.size();i++)
		{
			for(int j=0;j<tagIDs.size();j++)
			{
				double sum1=0;
				double sum2=0;
				for(int t=0;t<tagIDs.size();t++)
				{
					double weight=JSD(tag_distributions[j],tag_distributions[t]);
					weight=1-weight/Math.log(2);
					sum1+=weight*tag_ratings[i][t];
					sum2+=weight;
				}
				pstmt.setInt(1, userIDs.get(i));
				pstmt.setInt(2, j);
				pstmt.setDouble(3, sum1/sum2);
				pstmt.executeUpdate();
				count++;
				System.out.println(count);
				if(count%10000==9999)
				{
					f.conn.commit();
				}
			}
		}
		f.conn.commit();
		f.conn.close();
	}
	
	private static double JSD(double[] p, double[] q) {
		double[] m=new double[p.length];
		for(int i=0;i<p.length;i++)
		{
			m[i]=(p[i]+q[i])/2;
		}
		double s1=D(p,m)/2;
		double s2=D(q,m)/2;
		return s1+s2;
	}

	private static double D(double[] p, double[] q) {
		double sum=0;
		for(int i=0;i<p.length;i++)
		{
			if(p[i]!=0&&q[i]!=0)
			{
				sum+=p[i]*Math.log(p[i]/q[i]);
			}
		}
		return sum;
	}

}
