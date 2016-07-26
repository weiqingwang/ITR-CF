package sourcecluster;
import connector.Connector;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

//这个类完成在movie上对tag进行的聚类，将结果存储在topics中。
public class MovieCluster2 {
	static Connection conn;
	static double max_dis=0.03;
	public static void main(String[] args) throws ClassNotFoundException, SQLException{
		conn=Connector.getConnection();
		//先建表
		PreparedStatement pstmt=conn.prepareStatement("DROP TABLE IF EXISTS topics");
		pstmt.execute();
		pstmt.close();
		pstmt=conn.prepareStatement("create table topics(id int not null primary key auto_increment, topicID int not null, tag varchar(30) not null)");
		pstmt.execute();
		pstmt.close();
		
		pstmt=conn.prepareStatement("select distinct tagID from tagnames");
		ResultSet result=pstmt.executeQuery();
		ArrayList<Integer> tagIDs=new ArrayList<Integer>();
		while(result.next())
		{
			tagIDs.add(result.getInt(1));
		}
		result.close();
		pstmt.close();
		conn.setAutoCommit(false);
		pstmt=conn.prepareStatement("insert into topics(topicID,tag) values (?,?)");
		int topicID=-1;
		int count=0;
		while(!tagIDs.isEmpty()){
			//任选一个tag作为聚类的起点
			topicID++;
			int tagi=tagIDs.get(0);
			int tagj=0;
			pstmt.setInt(1, topicID);
			pstmt.setNString(2, getTag(tagi));
			pstmt.executeUpdate();
			tagIDs.remove(tagi);
			//在其余未聚类的样本中，找到与tagi距离小于0.03(任意取得，需要调整)的tag，将它们也聚在topicID中
			PreparedStatement pstmt1=conn.prepareStatement("select distinct tagID2 from distance where tagID1=? and distance<=?");
			pstmt1.setInt(1, tagi);
			pstmt1.setDouble(2, max_dis);
			result=pstmt1.executeQuery();
			while(result.next())
			{
				tagj=result.getInt(1);
				if(!tagIDs.contains(tagj))
					continue;
				pstmt.setInt(1, topicID);
				pstmt.setNString(2, getTag(tagj));
				pstmt.executeUpdate();
				tagIDs.remove(tagj);
				count++;
				if(count%1000==999)
				{
					conn.commit();
				}
			}
			result.close();
			pstmt.close();				
		}	
		conn.commit();
		conn.close();
	}
	
	//我们聚类的时候采用的是tagid，聚类结束后，数据库中要存储为tag，这个方法根据tagid来获得tag
	public static String getTag(int tagID)  throws ClassNotFoundException, SQLException{
		PreparedStatement pstmt=conn.prepareStatement("select distinct tag from tagnames where tagID=?");
		pstmt.setInt(1, tagID);
		ResultSet result=pstmt.executeQuery();
		String tag=result.getNString(1);
		result.close();
		pstmt.close();
		return tag;
	}
}
