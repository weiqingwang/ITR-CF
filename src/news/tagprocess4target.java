package news;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;


public class tagprocess4target {
	static ResultSet result;
	static PreparedStatement pstmt2;
	static Connection conn;
	public static void process() throws ClassNotFoundException, SQLException
	{
		ArrayList<Integer> tagIDs=new ArrayList<Integer>();
		Class.forName("com.mysql.jdbc.Driver");
		conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/movies", "root", "123456");
		pstmt2=conn.prepareStatement("select distinct tagID from tagnamestarget");
		result=pstmt2.executeQuery();
		while(result.next())
		{
			tagIDs.add(result.getInt(1));
		}
		result.close();
		pstmt2.close();
		conn.setAutoCommit(false);
		PreparedStatement pstmt=conn.prepareStatement("delete from distributionstarget");
		pstmt.execute();
		pstmt.close();
		pstmt=conn.prepareStatement("insert into distributionstarget(tagID1,tagID2,distribution) values (?,?,?)");
		int count=0;
		for(int i=0;i<tagIDs.size();i++)
		{
			int tagID1=tagIDs.get(i);
			for(int j=0;j<tagIDs.size();j++)
			{
				int tagID2=tagIDs.get(j);
				double distribution=0;
				ArrayList<Integer> movieIDs;
				movieIDs=new ArrayList<Integer>();
				pstmt2=conn.prepareStatement("select a.movieID from (select distinct movieID from tagnamestarget where tagID=?) as a,(select distinct movieID from tagnamestarget where tagID=?) as b where a.movieID=b.movieID");
				pstmt2.setInt(1, tagID1);
				pstmt2.setInt(2, tagID2);
				result=pstmt2.executeQuery();
				while(result.next())
				{
					movieIDs.add(result.getInt(1));
				}
				result.close();
				pstmt2.close();
				for(int ii=0;ii<movieIDs.size();ii++)
				{
					distribution+=q(tagID2,movieIDs.get(ii))*Q(tagID1,movieIDs.get(ii));
				}
				//System.out.println(distribution);
				if(distribution!=0)
				{
					pstmt.setInt(1, tagID1);
					pstmt.setInt(2, tagID2);
					pstmt.setDouble(3, distribution);
					pstmt.executeUpdate();
					System.out.println(count+":");
					System.out.println(distribution);
					count++;
				}
				if(count%50==49)
				{
					conn.commit();
				}
			}
		}
		conn.commit();
		conn.close();
	}

	private static double Q(int tagID1,int movieID) throws SQLException {
		// TODO Auto-generated method stub
		double weight;
		pstmt2=conn.prepareStatement("select weight from qqtarget where movieID=? and tagID=?");
		pstmt2.setInt(1, movieID);
		pstmt2.setInt(2, tagID1);
		result=pstmt2.executeQuery();
		if(result.next())
			weight=result.getDouble(1);
		else
			weight=0;
		result.close();
		pstmt2.close();
		return weight;
	}

	private static double q(int tagID2, int movieID) throws SQLException {
		// TODO Auto-generated method stub
		double weight;
		pstmt2=conn.prepareStatement("select weight from qtarget where movieID=? and tagID=?");
		pstmt2.setInt(1, movieID);
		pstmt2.setInt(2, tagID2);
		result=pstmt2.executeQuery();
		if(result.next())
		{
			weight=result.getDouble(1);
		}
		else
		{
			weight=0;
		}
		result.close();
		pstmt2.close();
		return weight;
	}

}
