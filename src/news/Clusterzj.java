package news;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;


public class Clusterzj {
	static double tag_distributions[][];
	static ArrayList<Integer> tagIDs;
	static double distances[][];
//	static int topicCount=110;//一共的tagID的个数是1123
	public static void process() throws ClassNotFoundException, SQLException
	{
		Algorithm f=new Algorithm();
		//先拿到topictarget中的topic的数目
		f.pstmt=f.conn.prepareStatement("select count(distinct topicID) from topicstarget");
		ResultSet result=f.pstmt.executeQuery();
		result.next();
		int topicCount=result.getInt(1);
		result.close();
		f.pstmt.close();
		//tag
		f.pstmt=f.conn.prepareStatement("select distinct tagID from tagNamestarget");
		result=f.pstmt.executeQuery();
		tagIDs=new ArrayList<Integer>();
		while(result.next())
		{
			tagIDs.add(result.getInt(1));
		}
		System.out.println(tagIDs.size());
		int tagCount=tagIDs.size();
		result.close();
		f.pstmt.close();
		distances=new double[tagIDs.size()][tagIDs.size()];
		//tag-distributions
		tag_distributions=new double[tagIDs.size()][tagIDs.size()];
		f.pstmt=f.conn.prepareStatement("select tagID1,tagID2,distance from distancetarget");
		result=f.pstmt.executeQuery();
		while(result.next())
		{
			int tagID1=result.getInt(1);
			int tagID2=result.getInt(2);
			double distance=result.getDouble(3);
			distances[tagID1][tagID2]=distance;
		}
		result.close();
		f.pstmt.close();
		ArrayList<tagcluster> clusters=new ArrayList<tagcluster>();
		for(int i=0;i<tagIDs.size();i++)
		{
			ArrayList<Integer> ids=new ArrayList<Integer>();
			ids.add(i);
			tagcluster t=new tagcluster();
			t.cluster=ids;
			clusters.add(t);
		}
		for(int i=0;i<tagCount-topicCount;i++)
		{
			System.out.println(i);
			merge(clusters);
		}
		System.out.println(clusters.size());
		f.pstmt=f.conn.prepareStatement("select userID,movieID,tagID,tag from tagnamestarget");
		f.pstmt2=f.conn.prepareStatement("delete from tagnamesMzj");
		f.pstmt2.execute();
		f.pstmt2.close();
		f.pstmt2=f.conn.prepareStatement("insert into tagnamesMzj(userID,movieID,tagID,topic,tag) values (?,?,?,?,?)");
		f.conn.setAutoCommit(false);
		result=f.pstmt.executeQuery();
		int count=0;
		while(result.next())
		{
			int userID=result.getInt(1);
			int movieID=result.getInt(2);
			int tagID=result.getInt(3);
			String tag=result.getString(4);
			int topic=0;
			for(int i=0;i<clusters.size();i++)
			{
				if(clusters.get(i).cluster.contains(tagID))
				{
					topic=i;
					break;
				}
			}
			f.pstmt2.setInt(1, userID);
			f.pstmt2.setInt(2, movieID);
			f.pstmt2.setInt(3, tagID);
			f.pstmt2.setInt(4, topic);
			f.pstmt2.setString(5, tag);
			f.pstmt2.executeUpdate();
			count++;
			System.out.println(count);
			if(count%10000==9999)
			{
				f.conn.commit();
			}
		}
		result.close();
		f.pstmt.close();
		f.conn.commit();
		f.conn.close();
	}
	
	private static void merge(ArrayList<tagcluster> clusters) {
		// TODO Auto-generated method stub
		double min=1;
		int minI=-1;
		int minJ=-1;
		for(int i=0;i<clusters.size();i++)
		{
			for(int j=i+1;j<clusters.size();j++)
			{
				double d=distance(clusters.get(i),clusters.get(j));
				if(d<min)
				{
					min=d;
					minI=i;
					minJ=j;
				}
			}
		}
		clusters.get(minI).cluster.addAll(clusters.get(minJ).cluster);
		clusters.remove(minJ);
	}

	private static double distance(tagcluster tagcluster1, tagcluster tagcluster2) {
		// TODO Auto-generated method stub
		double sum=0;
		int count=0;
		for(int i=0;i<tagcluster1.cluster.size();i++)
		{
			for(int j=0;j<tagcluster2.cluster.size();j++)
			{
				sum+=distances[tagcluster1.cluster.get(i)][tagcluster2.cluster.get(j)];
				count++;
			}
		}
		return sum/count;
	}

//	private static double JSD(double[] p, double[] q) {
//		double[] m=new double[p.length];
//		for(int i=0;i<p.length;i++)
//		{
//			m[i]=(p[i]+q[i])/2;
//		}
//		double s1=D(p,m)/2;
//		double s2=D(q,m)/2;
//		return Math.sqrt(s1+s2);
//	}
//
//	private static double D(double[] p, double[] q) {
//		double sum=0;
//		for(int i=0;i<p.length;i++)
//		{
//			if(p[i]!=0&&q[i]!=0)
//			{
//				sum+=p[i]*Math.log(p[i]/q[i]);
//			}
//		}
//		return sum;
//	}
}
