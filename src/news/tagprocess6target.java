package news;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;


public class tagprocess6target {
	static ArrayList<Integer> tagIDs;
	static ArrayList<Integer> movieIDs;
	//static double tag_distributions[][];
	public static void process() throws ClassNotFoundException, SQLException
	{
		Algorithm f=new Algorithm();
		//movie
		f.pstmt=f.conn.prepareStatement("select distinct movieID from tagNamestarget");
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
		f.conn.setAutoCommit(false);
		PreparedStatement pstmt=f.conn.prepareStatement("delete from qtarget");
		pstmt.execute();
		pstmt.close();
		pstmt=f.conn.prepareStatement("insert into qtarget(tagID,movieID,weight) values (?,?,?)");
		int count=0;
		for(int i=0;i<movieIDs.size();i++)
		{
			for(int j=0;j<tagIDs.size();j++)
			{
				int n=0;
				int N=0;
				f.pstmt=f.conn.prepareStatement("select tagID from tagnamestarget where movieID=?");
				f.pstmt.setInt(1, movieIDs.get(i));
				result=f.pstmt.executeQuery();
				while(result.next())
				{
					int tagID=result.getInt(1);
					n++;
					if(tagID==tagIDs.get(j))
						N++;
				}
				result.close();
				f.pstmt.close();
				double weight=(N+0.0)/n;
				if(weight!=0)
				{
					pstmt.setInt(2,movieIDs.get(i));
					pstmt.setInt(1,tagIDs.get(j));
					pstmt.setDouble(3,weight);
					pstmt.executeUpdate();
					count++;
					System.out.println(count);
				}
				if(count%1000==999)
				{
					f.conn.commit();
				}
			}
		}
		f.conn.commit();
		f.conn.close();
	}
}
