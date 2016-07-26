package news;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

//计算tag在一个movie中的权重，即：w(m,t)
public class tagprocess5_new {
	static ArrayList<Integer> tagIDs;
	static ArrayList<Integer> movieIDs;
	static double tag_distributions[][];
	public static void process() throws ClassNotFoundException, SQLException
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
		//tag
		f.pstmt=f.conn.prepareStatement("select distinct tagID from tagNamestarget");
		result=f.pstmt.executeQuery();
		tagIDs=new ArrayList<Integer>();
		while(result.next())
		{
			tagIDs.add(result.getInt(1));
		}
		System.out.println(tagIDs.size());
		result.close();
		f.pstmt.close();
		//distributions
		f.pstmt=f.conn.prepareStatement("select tagID1,tagID2,distribution from distributionstarget");
		result=f.pstmt.executeQuery();
		tag_distributions=new double[tagIDs.size()][tagIDs.size()];
		while(result.next())
		{
			int i=result.getInt(1);
			int j=result.getInt(2);
			double distribution=result.getDouble(3);
			tag_distributions[i][j]=distribution;
		}
		result.close();
		f.pstmt.close();
		//tag-weights
		f.conn.setAutoCommit(false);
		PreparedStatement pstmt=f.conn.prepareStatement("delete from tagweights");
		pstmt.execute();
		pstmt.close();
		pstmt=f.conn.prepareStatement("insert into tagweights(movieID,tagID,weight) values (?,?,?)");
		int count=0;
		for(int i=0;i<movieIDs.size();i++)
		{
			for(int j=0;j<tagIDs.size();j++)
			{
				int movieID=movieIDs.get(i);
				int tagID=tagIDs.get(j);
				PreparedStatement pstmt2=f.conn.prepareStatement("select tagID from tagNamestarget where movieID=?");
				pstmt2.setInt(1, movieID);
				result=pstmt2.executeQuery();
				double weight=0;
				int num=0;
				while(result.next())
				{
					num++;
					weight+=tag_distributions[result.getInt(1)][j];

				}
				result.close();
				pstmt2.close();
				if(weight!=0)
				{
					weight=weight/num;
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
}
