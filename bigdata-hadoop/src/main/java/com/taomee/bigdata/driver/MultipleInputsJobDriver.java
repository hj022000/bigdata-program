package com.taomee.bigdata.driver;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

/**
 * hadoop 2.0通用driver API
 * @author looper
 * @date 2016年10月11日
 */
public class MultipleInputsJobDriver extends Configured implements Tool {

	private static final Logger LOG = LoggerFactory
			.getLogger(MultipleInputsJobDriver.class);

	private static int printUsage() {
		System.out
				.println("MultipleInputsJobDriver "
						+ " -jobName <job name> "
						+ " -inFormat <input format> "
						+ " -outFormat <output format >"
						+ " -reducerClass <reducer class> "
						+ " -combinerClass <combiner class> "
						+ " -outKey <output key class> "
						+ " -outValue <output value class> "
						+ " -gameInfo <game info> "
						+ " -addInput <input path>,<mapper class> "
						+ " [-addMos <name>,<output format>,<output key class>,<output value class>] "
						+ " [-setPartitioner <partitioner class> ] "
						+ " -output <output path> ");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public int run(String[] args) throws Exception {

		Job job = new Job(getConf());
		List<Path> inputPaths = new ArrayList<Path>();
		List<Class<? extends Mapper>> inputMapClasses = new ArrayList<Class<? extends Mapper>>();
		Path outputPath = null;
		Class<? extends Reducer> reducerClass = null;
		Class<? extends Reducer> combinerClass = null;
		Class<? extends WritableComparable> outputKeyClass = Text.class;
		Class<? extends Writable> outputValueClass = Text.class;

		Class<? extends InputFormat> inputFormatClass = job
				.getInputFormatClass();

		Class<? extends OutputFormat> outputFormatClass = job
				.getOutputFormatClass();
		List<String> mosNames = new ArrayList<String>();
		List<Class<? extends OutputFormat>> mosOutputFormats = new ArrayList<Class<? extends OutputFormat>>();
		List<Class<? extends WritableComparable>> mosOutputKeyClasses = new ArrayList<Class<? extends WritableComparable>>();
		List<Class<? extends Writable>> mosOutputValueClasses = new ArrayList<Class<? extends Writable>>();

		Class<? extends Partitioner> partitionerClass = null;

		List<String> gameId = new ArrayList<String>();

		List<String> otherArgs = new ArrayList<String>();
		String jobName = "";
		int i = 0;

		for (i = 0; i < args.length; i++) {
			System.out.println("args " + i + " = " + args[i]);
		}

		try {
			for (i = 0; i < args.length; i++) {

				if (args[i].equals("-jobName")) {

					jobName = args[++i];

				} else if (args[i].equals("-addInput")) {

					String val = args[++i];
					System.err.println("val " + val);
					String[] valItems = val.split(",");
					if (valItems.length < 2) {
						throw new IllegalArgumentException(
								"invalid add input format");
					}
					inputPaths.add(new Path(valItems[0]));

					System.out.println("valItems[1]:" + valItems[1]);
					inputMapClasses.add(Class.forName(valItems[1]).asSubclass(
							Mapper.class));

					// LOG.info("Map类:" +
					// Class.forName(valItems[1]).toString());

				} else if (args[i].equals("-output")) {

					outputPath = new Path(args[++i]);
					LOG.info("outputPath:" + outputPath);
				} else if (args[i].equals("-reducerClass")) {

					reducerClass = Class.forName(args[++i]).asSubclass(
							Reducer.class);
				} else if (args[i].equals("-outKey")) {

					outputKeyClass = Class.forName(args[++i]).asSubclass(
							WritableComparable.class);
				} else if (args[i].equals("-outValue")) {

					outputValueClass = Class.forName(args[++i]).asSubclass(
							Writable.class);
				} else if (args[i].equals("-inFormat")) {

					inputFormatClass = Class.forName(args[++i]).asSubclass(
							InputFormat.class);
				} else if (args[i].equals("-outFormat")) {

					outputFormatClass = Class.forName(args[++i]).asSubclass(
							OutputFormat.class);
				} else if (args[i].equals("-gameInfo")) {
					String gameinfo_temp = args[++i];
					// transmit gameinfo to job
					// job.set("GameInfo", gameinfo_temp);
					this.getConf().set("GameInfo", gameinfo_temp);
					String[] gameinfo = gameinfo_temp.split(",");
					if (gameinfo.length < 1) {
						throw new IllegalArgumentException(
								"invalid add gameInfo");
					}
					for (int j = 0; j < gameinfo.length; j++) {
						gameId.add(gameinfo[j]);
					}
				} else if (args[i].equals("-addMos")) {

					String val = args[++i];
					System.err.println("val " + val);
					String[] valItems = val.split(",");
					if (valItems.length < 4) {
						throw new IllegalArgumentException(
								"invalid add mos format");
					}
					for (int j = 0; j < gameId.size(); j++) {
						mosNames.add(valItems[0] + "G" + gameId.get(j));

						mosOutputFormats.add(Class.forName(valItems[1])
								.asSubclass(OutputFormat.class));
						mosOutputKeyClasses.add(Class.forName(valItems[2])
								.asSubclass(WritableComparable.class));
						mosOutputValueClasses.add(Class.forName(valItems[3])
								.asSubclass(Writable.class));
					}
					mosNames.add(valItems[0]);
					mosOutputFormats.add(Class.forName(valItems[1]).asSubclass(
							OutputFormat.class));
					mosOutputKeyClasses.add(Class.forName(valItems[2])
							.asSubclass(WritableComparable.class));
					mosOutputValueClasses.add(Class.forName(valItems[3])
							.asSubclass(Writable.class));

				} else if (args[i].equals("-combinerClass")) {
					combinerClass = Class.forName(args[++i]).asSubclass(
							Reducer.class);
				} else if (args[i].equals("-setPartitioner")) {
					partitionerClass = Class.forName(args[++i]).asSubclass(
							Partitioner.class);
				} else {
					otherArgs.add(args[i]);
				}
			}
		} catch (ArrayIndexOutOfBoundsException e) {
			System.out.println("missing param for " + args[i - 1]);
			return printUsage();
		} catch (ClassNotFoundException e) {
			System.out.println("class not found for " + args[i - 1]);
			return printUsage();
		} catch (IllegalArgumentException e) {
			System.out.println("illegal arguments:" + e.toString());
			return printUsage();
		}
		for (int j = 0; j < gameId.size(); j++) {
			mosNames.add("partG" + gameId.get(j));
			mosOutputFormats.add(outputFormatClass);
			mosOutputKeyClasses.add(outputKeyClass);
			mosOutputValueClasses.add(outputValueClass);
		}

		if (inputPaths.size() == 0) {
			System.out.println("no input paths");
			return printUsage();
		}

		if (reducerClass == null) {
			System.out.println("missing -reducerClass param");
			return printUsage();
		}

		if (outputPath == null) {
			System.out.println("missing -output param");
			return printUsage();
		}

		FileSystem fs = FileSystem.get(this.getConf());

		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		for (i = 0; i < mosNames.size(); i++) {
			System.err.println("add multi outputs '" + mosNames.get(i) + "'");
			MultipleOutputs.addNamedOutput(job, mosNames.get(i),
					mosOutputFormats.get(i), mosOutputKeyClasses.get(i),
					mosOutputValueClasses.get(i));
		}

		job.setOutputFormatClass(outputFormatClass);

		FileOutputFormat.setOutputPath(job, outputPath);

		for (i = 0; i < inputPaths.size(); i++) {
			MultipleInputs.addInputPath(job, inputPaths.get(i),
					inputFormatClass, inputMapClasses.get(i));
		}

		job.setJobName(jobName);
		job.setReducerClass(reducerClass);
		if (combinerClass != null) {
			System.err.println("set combiner class " + combinerClass);
			job.setCombinerClass(combinerClass);
		}
		job.setOutputKeyClass(outputKeyClass);
		job.setOutputValueClass(outputValueClass);

		if (partitionerClass != null) {
			System.err.println("set partitioner " + partitionerClass);
			job.setPartitionerClass(partitionerClass);
		}
		//需要设置jar的class,否则程序会包找不到对应jar的错误
		job.setJarByClass(getClass());
		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {

		int ret = ToolRunner.run(new MultipleInputsJobDriver(), args);
		System.exit(ret);
	}
}
