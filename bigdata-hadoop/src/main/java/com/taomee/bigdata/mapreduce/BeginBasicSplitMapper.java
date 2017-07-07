package com.taomee.bigdata.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author looper
 * @date 2017年1月16日 下午3:21:42
 * @project tms_hadoop BeginBasicSplitMapper
 */
public class BeginBasicSplitMapper extends Mapper<Object, Text, Text, Text> {

	/**
	 * 该方法在task只会被调用一次，可以用来初始化一些资源，比如从数据库里面读取一些配置信息
	 * 在这个方法当中，我们用来从tms数据库里面读取T_S_M_S四张表的信息
	 */


	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		/**
		 * 内部实现全都忽略掉
		 */

	}

	/**
	 * 执行日志Mapper的打散逻辑
	 */
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		/**
		 * 需要解析的日志格式
		 * 格式1:LogItemBean [strOp=sum, schemaId=4, serverId=1, cascadeValue=, opValues=[10], gameId=16]
		 * 格式2:LogItemBean [strOp=material, schemaId=1, serverId=6, cascadeValue=, opValues=[583895372, 10], gameId=16, materialId=130]
		 */
		//
		/**
		 * 内部实现暂时都忽略掉
		 */

	}
	
	private static void logMapOut()
	{
		
	}

	/**
	 * 在一次task任务的最后调用该方法，比如处理一些资源收回等一些任务
	 */
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		// dbTemplate.closeConn(dbTemplate.getConn());
		super.cleanup(context);
	}
}
