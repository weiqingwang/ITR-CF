package transfer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import connector.Connector;

public class BookCluster1 {

	static ResultSet result;
	static PreparedStatement pstmt;
	static Connection conn;
	public static void process()  throws ClassNotFoundException, SQLException{
		conn=Connector.getConnection();
		//先建表
		pstmt=conn.prepareStatement("DROP TABLE IF EXISTS distancetarget");
		pstmt.execute();
		pstmt.close();
		pstmt=conn.prepareStatement("create table distancetarget(id int not null primary key auto_increment, tagID1 int not null, tagID2 int not null , distance double not null)");
		pstmt.execute();
		pstmt.close();
		
		//构造特征向量矩阵
		pstmt=conn.prepareStatement("select count(distinct tagID) from tagnamestarget");
		result=pstmt.executeQuery();
		int tag_size=0;
		while(result.next())
		{
			tag_size=result.getInt(1);
		}
		result.close();
		pstmt.close();
		double[][] featureVector=new double[tag_size][tag_size];
		
		pstmt=conn.prepareStatement("select tagID1,tagID2,distribution from distributionstarget");
		result=pstmt.executeQuery();
		while(result.next()){
			featureVector[result.getInt(1)][result.getInt(2)]=result.getDouble(3);
		}
		result.close();
		pstmt.close();
		
		
		conn.setAutoCommit(false);
		PreparedStatement pstmt=conn.prepareStatement("insert into distancetarget(tagID1,tagID2,distance) values (?,?,?)");
		//下面开始利用特征向量计算JSD距离
		int count=0;
		for(int i=0;i<tag_size;i++){
			double[] vectorP=featureVector[i];
			double[] vectorQ;
			double[] vectorM=new double[tag_size];
			for(int j=i+1;j<tag_size;j++){
				double dpm=0,dqm=0;
				double jsd=0;
				vectorQ=featureVector[j];
				//先计算M
				for(int m=0;m<tag_size;m++){
					vectorM[m]=(vectorP[m]+vectorQ[m])/2;
				}
				//分别计算D(P||M)和D(Q||M)
				for(int d=0;d<tag_size;d++){
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
				pstmt.setInt(1, i);
				pstmt.setInt(2, j);
				pstmt.setDouble(3, jsd);
				pstmt.executeUpdate();
				count++;
				if(count%1000==999){
					conn.commit();
				}
			}
		}
		conn.commit();
		pstmt.close();
		conn.close();
	}

}
