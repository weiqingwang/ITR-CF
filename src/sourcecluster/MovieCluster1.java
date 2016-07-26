package sourcecluster;
import connector.Connector;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;


//该类主要的任务是利用JSD计算所有tag之间的距离，存储在wqwang.distance表中
public class MovieCluster1 {
	static ResultSet result;
	static PreparedStatement pstmt;
	static Connection conn;
	public static void main(String[] args)  throws ClassNotFoundException, SQLException{
		conn=Connector.getConnection();
		PreparedStatement pstmt1=conn.prepareStatement("select distribution from distributions where tagID1=? and tagID2=?");
		ResultSet result1;
		ArrayList<Integer> tagIDs=new ArrayList<Integer>();
		pstmt=conn.prepareStatement("select distinct tagID from tagnames");
		result=pstmt.executeQuery();
		while(result.next())
		{
			tagIDs.add(result.getInt(1));
		}
		result.close();
		pstmt.close();
		conn.setAutoCommit(false);
		PreparedStatement pstmt=conn.prepareStatement("insert into distance(tagID1,tagID2,distance) values (?,?,?)");
		double[][] featureVector=new double[tagIDs.size()][tagIDs.size()];
		//下面计算所有tag的特征向量
		for(int i=0;i<tagIDs.size();i++){
			int tagIDi=tagIDs.get(i);
			for(int j=0;j<tagIDs.size();j++){
				int tagIDj=tagIDs.get(j);
				pstmt1.setInt(1, tagIDi);
				pstmt1.setInt(2, tagIDj);
				result1=pstmt1.executeQuery();
				//说明二者的distribution是0
				if(result1==null){
					featureVector[i][j]=0;
				}
				while(result1.next()){
					featureVector[i][j]=result1.getDouble(1);
				}
			}
		}
		//下面开始利用特征向量计算JSD距离
		int count=0;
		for(int i=0;i<tagIDs.size();i++){
			int tagIDp=tagIDs.get(i);
			double[] vectorP=featureVector[i];
			double[] vectorQ;
			double[] vectorM=new double[tagIDs.size()];
			double dpm=0,dqm=0;
			double jsd=0;
			for(int j=i+1;j<tagIDs.size();j++){
				int tagIDq=tagIDs.get(j);
				vectorQ=featureVector[j];
				//先计算M
				for(int m=0;m<tagIDs.size();m++){
					vectorM[m]=(vectorP[m]+vectorQ[m])/2;
				}
				//分别计算D(P||M)和D(Q||M)
				for(int d=0;d<tagIDs.size();d++){
					//这边需要限制P和Q都不能等于0
					double temp=0;
					if(vectorP[d]==0)
						temp=0;
					else{
						temp=Math.log(vectorP[d]/vectorM[d]);
						temp=temp*vectorP[d];
					}
					dpm+=temp;
					if(vectorQ[d]==0)
						temp=0;
					else{
					temp=Math.log(vectorQ[d]/vectorM[d]);
					temp=temp*vectorQ[d];
					}
					dqm+=temp;
				}
				//计算JSD
				jsd=dpm/2+dqm/2;
				pstmt.setInt(1, tagIDp);
				pstmt.setInt(2, tagIDq);
				pstmt.setDouble(3, jsd);
				pstmt.executeUpdate();
				count++;
				if(count%50==49){
					conn.commit();
				}
			}
		}
		conn.commit();
		pstmt.close();
		conn.close();
	}
}
