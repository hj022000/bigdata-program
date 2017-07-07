package com.taomee.bigdata.mapreduce;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author looper
 * @date 2016年10月26日
 */
public class BeginBasicSplitReducer extends Reducer<Text, Text, Text, Text> {

	private MultipleOutputs mos;
	private String m_id;
	private String gid;//分游戏
	//private NullWritable
	private Text outkey=new Text();
	private Text outvalue=new Text(); 
	private Logger LOG = LoggerFactory.getLogger(BeginBasicSplitReducer.class);
	private SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
	private Date date = new Date(System.currentTimeMillis());
	private String defaultDate = sdf.format(date);//系统默认时间
	private String caldate = new String();
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		// super.setup(context);
		mos = new MultipleOutputs(context);
		//获取-D参数传递进来的时间参数
		caldate = context.getConfiguration().get("date",defaultDate);
		//String mapred_reduce_tasks = context.getConfiguration().get("mapred.reduce.tasks");
		//System.out.println("caldate:" +date2);
		//System.out.println("mapred_reduce_tasks:" +mapred_reduce_tasks);
		//Configuration conf = context.getConfiguration();
		
	}

	/**
	 * //设置key:gid \t schemaId \t serverId \t cascadeValue \t op
	   //设置value: opValues
	 */
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String [] keys=key.toString().split("\t");
		//LOG.info("key 的长度:"+keys.length);
		/**
		 * 1	material()
//		 * 2	count()
//		 * 3	distinct_count(key)
//		 * 4	sum(key)
//		 * 5	max(key)
//		 * 6	min(key)
//		 * 7	assign(key)
		 */
		for (Text val : values) {
			
			switch (keys[4].toString()) {
			case "count":
				
			case "distinct_count":
				
			case "sum":
			
			case "max":
			 
			case "mix":
			
			case "assign":				
				outkey.set(String.format("%s\t%s\t%s", keys[1],keys[2],keys[3]));			
				outvalue.set(val.toString());
				gid=keys[0];
				mos.write(outkey, outvalue, keys[4]+"/"+gid.toString()+"/"+caldate+"/part");
				break;
				
			case "material":
				outkey.set(String.format("%s\t%s",keys[2],keys[3]));
				outvalue.set(val.toString());
				m_id =keys[1];
				gid=keys[0];
				mos.write(outkey, outvalue, "material/"+m_id+"/"+gid.toString()+"/"+caldate+"/part");
				break;
			default:
				break;
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
		mos.close();
	}

}
