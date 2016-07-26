package news;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

//计算topic在一个movie中的权重，即：w(m,tt)
//先构建temptagweights,里面的信息是topicID,tagID,movieID,tagWeight
public class TagProcess6_newzj {
	public static void process() throws ClassNotFoundException, SQLException{
		ArrayList<Integer> topicIDs=new ArrayList<Integer>();
		Algorithm f=new Algorithm();
		//先拿出所有的tagID和topicID的对应信息
		f.pstmt=f.conn.prepareStatement("select topicID from topicstargetzj order by tagID");
		ResultSet result=f.pstmt.executeQuery();
		topicIDs=new ArrayList<Integer>();
		while(result.next())
		{
			topicIDs.add(result.getInt(1));
		}
		result.close();
		f.pstmt.close();
		
		f.conn.setAutoCommit(false);
		f.pstmt=f.conn.prepareStatement("delete from temptagweightszj");
		f.pstmt.execute();
		f.pstmt.close();
		f.pstmt=f.conn.prepareStatement("insert into temptagweightszj(topicID,tagID,movieID,tagweight) values(?,?,?,?)");
		f.pstmt2=f.conn.prepareStatement("select tagID,movieID,weight from tagweights");
		result=f.pstmt2.executeQuery();
		int count=0;
		while(result.next()){
			int tagID=result.getInt(1);
			f.pstmt.setInt(1, topicIDs.get(tagID));
			f.pstmt.setInt(2, tagID);
			f.pstmt.setInt(3, result.getInt(2));
			f.pstmt.setDouble(4, result.getDouble(3));
			f.pstmt.executeUpdate();
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
