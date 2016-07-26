package transfer;
import connector.Connector;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

//��������movie����õ���topics����target�����tag���о��࣬������洢��topicstarget�С�˼·�����һ��tag�Ѿ�������movies�У������κ����飬��������ڡ�����
public class BookCluster3 {
	static Connection conn;
	static double max_dis=0.4;//��ʾ���þ�����ֵ
	public static void main(String[] args) throws ClassNotFoundException, SQLException{
		conn=Connector.getConnection();
		int countTopic=0;
		//�ȵó��ִ��topic������
		PreparedStatement pstmt1=conn.prepareStatement("select count(distinct topicID) from topicstarget");
		pstmt1.executeQuery();
		ResultSet result1=pstmt1.executeQuery();
		countTopic=0;
		while(result1.next()){
			countTopic=result1.getInt(1);
		}
		result1.close();
		pstmt1.close();
		
		conn.setAutoCommit(false);
		PreparedStatement pstmt=conn.prepareStatement("insert into topicstarget(topicID,tag) values (?,?)");
		pstmt1=conn.prepareStatement("select distinct tag,tagID from tagnamestarget");
		result1=pstmt1.executeQuery();
		while(result1.next()){
			String tagi=result1.getString(1);
			int tagi_id=result1.getInt(2);
			PreparedStatement pstmt2=conn.prepareStatement("select topicID from topicstarget where tag=?");
			pstmt2.setString(1, tagi);
			ResultSet result2=pstmt2.executeQuery();
			double mindis=1;//��С����
			int mini_topic=0;//ָʾ������С��topicid
			int count=0;
			if(!result2.next()){//˵����tagû�д��������е�topic��
				//����tagi��ÿһ��topic�ľ��룬�����ҳ�������С��topic
				System.out.println(tagi);
				pstmt2.close();
				//
				for(int index_topic=0;index_topic<=countTopic;index_topic++){
					int count_tag=0;
					double dis_topic=0;
					//������Ҫȡ����topic�е�����tag
					pstmt2=conn.prepareStatement("select tag from topicstarget where topicID=?");
					pstmt2.setInt(1, index_topic);
					result2=pstmt2.executeQuery();
					while(result2.next()){
						//���ڸ�topic��ÿһ��tagj����JSD(tagi,tagj)
						double jsdij=0;
						String tagj=result2.getString(1);
						int tagj_id=getTagID(tagj);
						PreparedStatement pstmt3=conn.prepareStatement("select distance from distancetarget where tagID1=? and tagID2=?");
						pstmt3.setInt(1, Math.min(tagi_id, tagj_id));
						pstmt3.setInt(2, Math.max(tagi_id, tagj_id));
						ResultSet result3=pstmt3.executeQuery();
						while(result3.next()){
							jsdij=result3.getDouble(1);
						}
						result3.close();
						pstmt3.close();
						dis_topic+=jsdij;
						count_tag++;
					}
					result2.close();
					pstmt2.close();
					//����ƽ��JSD
					dis_topic=dis_topic/count_tag;
					//���tagi�͵�ǰ��topic�ľ���dis_topic����С�ľ��뻹С
					if(dis_topic<mindis){
						mindis=dis_topic;
						mini_topic=index_topic;
					}
				}
				//�����С�ľ���Ҳ������ֵ(��ʱ��Ϊ0.4�����Ե��ڵ�),����topics�м���һ���µ�topicid��tagi
				if(mindis>max_dis){
					pstmt.setInt(1, ++countTopic);	
				}
				else{
					pstmt.setInt(1, mini_topic);
				}
				pstmt.setString(2, tagi);
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
	
	//���Ǿ����ʱ����õ���tagid,�����������tag�����tagid
	public static int getTagID(String tag)  throws ClassNotFoundException, SQLException{
		PreparedStatement pstmt=conn.prepareStatement("select distinct tagID from tagnamestarget where tag=?");
		pstmt.setString(1, tag);
		ResultSet result=pstmt.executeQuery();
		int tagID=-1;
		while(result.next()){
			tagID=result.getInt(1);
		}
		result.close();
		pstmt.close();
		return tagID;
	}
}
