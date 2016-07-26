package transfer;

import connector.Connector;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
//该类将movie上面的topic迁移到book上面，只从中抽取book上面有的tag
public class BookCluster2_new {
	static Connection conn;
	public static void process() throws ClassNotFoundException, SQLException{
		conn=Connector.getConnection();
		//先建表
		PreparedStatement pstmt=conn.prepareStatement("DROP TABLE IF EXISTS topicstarget");
		pstmt.execute();
		pstmt.close();
		pstmt=conn.prepareStatement("create table topicstarget(id int not null primary key auto_increment, topicID int not null, tagID int not null, tag varchar(30) not null)");
		pstmt.execute();
		pstmt.close();
		
		pstmt=conn.prepareStatement("select distinct tag from tagnamestarget order by tagID");
		ResultSet result=pstmt.executeQuery();
		ArrayList<String> tags_target=new ArrayList<String>();
		while(result.next()){
			tags_target.add(result.getString(1));
		}
		result.close();
		pstmt.close();
		
		PreparedStatement pstmt1=conn.prepareStatement("select tag,topicID from topics order by topicID asc");
		ResultSet result1=pstmt1.executeQuery();
		conn.setAutoCommit(false);
		pstmt=conn.prepareStatement("insert into topicstarget(topicID,tag,tagID) values (?,?,?)");
		int topicID=-1;
		int last_topicID=0;
		int count=0;
		while(result1.next()){
			if(!tags_target.contains(result1.getString(1))){
				continue;
			}
			else{
				if(topicID==-1)
					topicID=0;
				else if(result1.getInt(2)!=last_topicID)
					topicID++;
				pstmt.setInt(1, topicID);
				String tagtemp=result1.getString(1);
				pstmt.setString(2, tagtemp);
				pstmt.setInt(3, tags_target.indexOf(tagtemp));
				pstmt.executeUpdate();
				count++;
				last_topicID=topicID;
			}
			if(count%100==99)
				conn.commit();
		}
		conn.commit();
		result1.close();
		pstmt1.close();
		result.close();
		pstmt.close();
		conn.close();
	}
}
