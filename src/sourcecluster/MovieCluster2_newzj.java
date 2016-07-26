package sourcecluster;
import connector.Connector;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

//这个类完成从tagnamesm中抽取topic和tag，将结果存储在topics中，分别对应topicID和tag。
public class MovieCluster2_newzj {
	static Connection conn;
	public static void process() throws ClassNotFoundException, SQLException{
		conn=Connector.getConnection();
		PreparedStatement pstmt=conn.prepareStatement("select distinct topic,tag,tagID from tagnamesmzj");
		ResultSet result=pstmt.executeQuery();
		ArrayList<String> tags=new ArrayList<String>();
		PreparedStatement pstmt1=conn.prepareStatement("insert into topicstargetzj(topicID,tag,tagID) values (?,?,?)");
		conn.setAutoCommit(false);
		int count=0;
		while(result.next())
		{
			int topicID=result.getInt(1);
			String tag=result.getString(2);
			int tagID=result.getInt(3);
			if(tags.contains(tag))
				continue;
			tags.add(tag);
			pstmt1.setInt(1, topicID);
			pstmt1.setString(2, tag);
			pstmt1.setInt(3, tagID);
			pstmt1.execute();
			count++;
			if(count%1000==999)
				conn.commit();
		}
		result.close();
		pstmt.close();
		pstmt1.close();
		conn.commit();
		conn.close();
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException{
		process();
	}
}
