package transfer;
import connector.Connector;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import news.Main;

//该类利用movie上面得到的topics，对target上面的tag进行聚类，结果还存储在topicstarget中。思路是如果一个tag已经存在于movies中，则不作任何事情，如果不存在。。。
public class BookCluster3_new {
	static Connection conn;
	public static void process() throws ClassNotFoundException, SQLException{
		conn=Connector.getConnection();
		double max_dis=Main.max_dis;
		int countTopic=0;
		//先得出现存的topic的数量
		PreparedStatement pstmt1=conn.prepareStatement("select count(distinct topicID) from topicstarget");
		pstmt1.executeQuery();
		ResultSet result1=pstmt1.executeQuery();
		countTopic=0;
		while(result1.next()){
			countTopic=result1.getInt(1);
		}
		result1.close();
		pstmt1.close();
		
		pstmt1=conn.prepareStatement("select count(distinct tagID) from tagnamestarget");
		pstmt1.executeQuery();
		result1=pstmt1.executeQuery();
		int countTags=0;
		while(result1.next()){
			countTags=result1.getInt(1);
			//System.out.println(countTags);
		}
		result1.close();
		pstmt1.close();
		
		double[][] distances=new double[countTags][countTags];
		
		pstmt1=conn.prepareStatement("select tagID1,tagID2,distance from distancetarget order by tagID1");
		pstmt1.executeQuery();
		result1=pstmt1.executeQuery();
		while(result1.next()){
			int tagID1=result1.getInt(1);
			int tagID2=result1.getInt(2);
			double distance=result1.getDouble(3);
			distances[tagID1][tagID2]=distance;
		}
		result1.close();
		pstmt1.close();
		
		ArrayList<String> tagnames=new ArrayList<String>();
		pstmt1=conn.prepareStatement("select distinct tag from tagnamestarget order by tagID");
		pstmt1.executeQuery();
		result1=pstmt1.executeQuery();
		while(result1.next()){
			tagnames.add(result1.getString(1));
		}
		result1.close();
		pstmt1.close();
		
		conn.setAutoCommit(false);
		PreparedStatement pstmt=conn.prepareStatement("insert into topicstarget(topicID,tag,tagID) values (?,?,?)");
		pstmt1=conn.prepareStatement("select distinct tag,tagID from tagnamestarget");
		result1=pstmt1.executeQuery();
		while(result1.next()){
			String tagi=result1.getString(1);
			int tagi_id=result1.getInt(2);
			PreparedStatement pstmt2=conn.prepareStatement("select topicID from topicstarget where tag=?");
			pstmt2.setString(1, tagi);
			ResultSet result2=pstmt2.executeQuery();
			double mindis=1;//最小距离
			int mini_topic=0;//指示距离最小的topicid
			int count=0;
			if(!result2.next()){//说明该tag没有存在于现有的topic中
				//计算tagi和每一个topic的距离，从中找出距离最小的topic
				System.out.println(tagi);
				pstmt2.close();
				//
				for(int index_topic=0;index_topic<=countTopic;index_topic++){
					int count_tag=0;
					double dis_topic=0;
					//首先需要取出该topic中的所有tag
					pstmt2=conn.prepareStatement("select tag from topicstarget where topicID=?");
					pstmt2.setInt(1, index_topic);
					result2=pstmt2.executeQuery();
					while(result2.next()){
						//对于该topic中每一个tagj计算JSD(tagi,tagj)
						double jsdij=0;
						String tagj=result2.getString(1);
						int tagj_id=tagnames.indexOf(tagj);
						//System.out.println(Math.min(tagi_id, tagj_id));
						//System.out.println(Math.max(tagi_id, tagj_id));
						jsdij=distances[Math.min(tagi_id, tagj_id)][Math.max(tagi_id, tagj_id)];
						dis_topic+=jsdij;
						count_tag++;
					}
					result2.close();
					pstmt2.close();
					//计算平均JSD
					dis_topic=dis_topic/count_tag;
					//如果tagi和当前的topic的距离dis_topic比最小的距离还小
					if(dis_topic<mindis){
						mindis=dis_topic;
						mini_topic=index_topic;
					}
				}
				//如果最小的距离也大于阈值(暂时设为0.4，可以调节的),则向topics中加入一个新的topicid，tagi
				if(mindis>max_dis){
					pstmt.setInt(1, countTopic);	
					countTopic++;
				}
				else{
					pstmt.setInt(1, mini_topic);
				}
				pstmt.setString(2, tagi);
				pstmt.setInt(3, tagnames.indexOf(tagi));
				pstmt.executeUpdate();
				count++;
			}
			else{
				result2.close();
				pstmt2.close();
			}
//			if(count%1000==999)
				conn.commit();
		}
		conn.commit();
		result1.close();
		pstmt1.close();
		pstmt.close();
		conn.close();
	}
}
