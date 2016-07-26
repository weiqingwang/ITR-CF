package news;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;


public class tagprocess7 {
	static ArrayList<Integer> tagIDs;
	static ArrayList<Integer> movieIDs;
	static double tag_distributions[][];
	public static void process() throws ClassNotFoundException, SQLException
	{
		Algorithm f=new Algorithm();
		//movie
		f.pstmt=f.conn.prepareStatement("select distinct movieID from tagNames");
		ResultSet result=f.pstmt.executeQuery();
		movieIDs=new ArrayList<Integer>();
		while(result.next())
		{
			movieIDs.add(result.getInt(1));
		}
		result.close();
		f.pstmt.close();
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
		f.conn.setAutoCommit(false);
		PreparedStatement pstmt=f.conn.prepareStatement("delete from qq");
		pstmt.execute();
		pstmt.close();
		pstmt=f.conn.prepareStatement("insert into qq(tagID,movieID,weight) values (?,?,?)");
		int count=0;
		for(int i=0;i<movieIDs.size();i++)
		{
			for(int j=0;j<tagIDs.size();j++)
			{
				int n=0;
				int N=0;
				f.pstmt=f.conn.prepareStatement("select count(*) from tagnames where movieID=? and tagID=?");
				f.pstmt.setInt(1, movieIDs.get(i));
				f.pstmt.setInt(2, tagIDs.get(j));
				result=f.pstmt.executeQuery();
				result.next();
				N=result.getInt(1);
				result.close();
				f.pstmt.close();
				f.pstmt=f.conn.prepareStatement("select count(*) from tagnames where tagID=?");
				f.pstmt.setInt(1, tagIDs.get(j));
				result=f.pstmt.executeQuery();
				result.next();
				n=result.getInt(1);
				result.close();
				f.pstmt.close();
				double weight=(N+0.0)/n;
				if(weight!=0)
				{
					System.out.println(weight);
					pstmt.setInt(2,movieIDs.get(i));
					pstmt.setInt(1,tagIDs.get(j));
					pstmt.setDouble(3,weight);
					pstmt.executeUpdate();
					count++;
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
