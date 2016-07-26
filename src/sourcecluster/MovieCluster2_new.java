package sourcecluster;
import connector.Connector;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

//这个类完成从tagnamesm中抽取topic和tag，将结果存储在topics中，分别对应topicID和tag。
public class MovieCluster2_new {
	static Connection conn;
	public static void process() throws ClassNotFoundException, SQLException{
		conn=Connector.getConnection();
		//先建表
		PreparedStatement pstmt=conn.prepareStatement("DROP TABLE IF EXISTS topics");
		pstmt.execute();
		pstmt.close();
		pstmt=conn.prepareStatement("create table topics(id int not null primary key auto_increment, topicID int not null, tag varchar(30) not null)");
		pstmt.execute();
		pstmt.close();
		
		pstmt=conn.prepareStatement("select topic,tag from tagnamesm");
		ResultSet result=pstmt.executeQuery();
		ArrayList<String> tags=new ArrayList<String>();
		PreparedStatement pstmt1=conn.prepareStatement("insert into topics(topicID,tag) values (?,?)");
		conn.setAutoCommit(false);
		int count=0;
		while(result.next())
		{
			int topicID=result.getInt(1);
			String tag=result.getString(2);
			if(tags.contains(tag))
				continue;
			tags.add(tag);
			pstmt1.setInt(1, topicID);
			pstmt1.setString(2, tag);
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
	
}
