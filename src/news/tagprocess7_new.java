package news;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

//计算topic在一个movie中的权重，即：w(m,tt)
//利用temptagweights里面的信息topicID,tagID,movieID,tagWeight
//select sum(tagweight),topicID from temptagweights group by topicID order by topicID;
public class tagprocess7_new {
	public static void process() throws ClassNotFoundException, SQLException{
		//先拿出所有topic中的tag数目
		Algorithm f=new Algorithm();
		ArrayList<Integer> counttags=new ArrayList<Integer>();
		f.pstmt=f.conn.prepareStatement("select count(distinct tag) from topicstarget group by topicID order by topicID");
		ResultSet result=f.pstmt.executeQuery();
		while(result.next())
		{
			counttags.add(result.getInt(1));
		}
		result.close();
		f.pstmt.close();
	
		f.conn.setAutoCommit(false);
		f.pstmt=f.conn.prepareStatement("delete from topicweights");
		f.pstmt.execute();
		f.pstmt.close();
		f.pstmt=f.conn.prepareStatement("insert into topicweights(topicID,movieID,weight) values(?,?,?)");
		f.pstmt2=f.conn.prepareStatement("select sum(tagweight),topicID,movieID from temptagweights group by topicID,movieID");
		result=f.pstmt2.executeQuery();
		int count=0;
		while(result.next()){
			int topicID=result.getInt(2);
			f.pstmt.setInt(1, topicID);
			f.pstmt.setInt(2, result.getInt(3));
			f.pstmt.setDouble(3, result.getDouble(1)/counttags.get(topicID));
			f.pstmt.execute();
			count++;
			if(count%1000==999)
				f.conn.commit();
		}
		f.conn.commit();
		result.close();
		f.pstmt.close();
		f.pstmt2.close();
		f.conn.close();
	}
}
