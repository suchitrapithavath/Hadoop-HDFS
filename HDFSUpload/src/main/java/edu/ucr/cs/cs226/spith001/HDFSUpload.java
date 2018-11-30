package edu.ucr.cs.cs226.spith001;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.net.URI;


public class HDFSUpload 
{
	public static void main(String[] args) throws Exception {
	
	String localfile = args[0];//localfile name read from command line
	String hdfsfile = args[1];//hdfsfile name read from command line
	File f = new File(args[0]);//local file creation
	File d = new File(args[0]);
	Configuration conf = new Configuration();// creating configuration reference
	InputStream in;
	FileSystem fs;
	OutputStream out;
	FSDataInputStream ra;
	Random rand = new Random();//random class is used to generate random numbers
	int n=0;
	Path path =new Path(args[1]);//getting path of  hdfs file
	if(!f.exists())//checking the existence of local file
	 {
	    System.out.println("Local file doesnot exist");
	 }
	else{	
	in = new BufferedInputStream(new FileInputStream(localfile));//reading the content of local file
	fs = FileSystem.get(URI.create(hdfsfile), conf);//creating hdfs filesystem reference
	if(fs.exists(path))//checking for hdfs file existence
	{
	  System.out.println("File already exists in HDFS");
	}
	else{
	try{
	
	  out = fs.create(new Path(hdfsfile));//creating file in hdfs
	  long lhstart = System.currentTimeMillis();//starting the time to check HDFS file copy performance 
           
	  IOUtils.copyBytes(in, out, 4096, true);//copying the localfile content to hdfs file 
	  long lhend = System.currentTimeMillis(); //ending the time 
	  System.out.println("The total time taken to copy file from local to HDFS-" +
	          (lhend - lhstart) + "ms");
	
	}
	
	catch(IOException e)
	{
	  System.out.println("File couldnot be created");
	  e.printStackTrace();
	}
	long streamstartcopy=System.currentTimeMillis();//start time 
	copyFileUsingStream(f, d);//function to read hdfs file
	long streamendcopy =System.currentTimeMillis();//end time
	System.out.println("The total time taken to copy file from local to local-" +
	        (streamendcopy - streamstartcopy) + "ms");
	
	//code to read file sequencially
                long start_seq = System.currentTimeMillis();
                BufferedReader br = new BufferedReader(new FileReader(f));//read local file for sequencial read
                String st;
                  while ((st = br.readLine()) != null) {//reading sequentially
                  }

                long end_seq = System.currentTimeMillis();
                System.out.println("total time taken to read file sequentially-"+(end_seq - start_seq)+"ms");
                long start_seq1 = System.currentTimeMillis();
                readSequentiallyhdfs(path);//readsequentialhdfs
                long end_seq1 = System.currentTimeMillis();
                System.out.println("total time taken to read HDFS sequentially-"+(end_seq1 - start_seq1)+"ms");

	//code to access position of file randomly
	long start = System.currentTimeMillis(); 
	for (int i =0;i<2000;i++) {// reading 2000 random position of local file
		n = rand.nextInt(4867);//generating random numbers
	    readFromFile(args[0], n, 1024);//function to read random position bytes
	}
	long end = System.currentTimeMillis(); 
	System.out.println("The total time taken by local 2000 random access-"+ (end - start) + "ms");
	
	long randomhdfsstart = System.currentTimeMillis();
	
	ra = fs.open(new Path(args[1]));//opening hdfs file
	for (int j =0;j<2000;j++)//reading 2000 random position of hdfs file
	{
		try {
	    byte[] b1 =new byte[2000];//byte array of size 2000
	    int r= rand.nextInt(4867);//generating random numbers
	    ra.seek(r);//seeking to random location
	    ra.read(b1,0,1024);//reading the bytes from random location
		}
		catch(IOException ex)
		{
	           ex.getMessage();
		}
	}
	ra.close();//closing the FSdatainputstream reference 
	long randomhdfsend = System.currentTimeMillis();
	System.out.println("The total time taken by HDFS to make 2000 random accesses-" +
	                    (randomhdfsend - randomhdfsstart) + "ms");
	

	
	
	 }
	}
}
private static void readFromFile(String filePath, int position, int size)//function to read random postion of file
		throws IOException {

	RandomAccessFile file = new RandomAccessFile(filePath, "r");
	try {
	file.seek(position);//seeking to random position of file
	byte[] bytes = new byte[size];
	file.read(bytes);//reading the bytes from random position
	file.close();
	}
	catch (IOException re)
	{
	       re.getMessage();
	}
	return;

}
public static void copyFileUsingStream(File source, File dest) throws Exception {//function to copy file locally
    InputStream is = null;
    OutputStream os = null;
    try {
        is = new FileInputStream(source);//source file
        os = new FileOutputStream(dest);//destination file
        byte[] buffer = new byte[1024];
        int length;
        while ((length = is.read(buffer)) > 0) {//copying the source file to dstination  
            os.write(buffer, 0, length);
        }
    } finally {
        is.close();
        os.close();
    }
}

public static void readSequentiallyhdfs(Path path) throws Exception {//function to read hdfs file sequentially 
	Configuration conf1 = new Configuration();
	FileSystem filesystem = FileSystem.get(conf1);
	if (!filesystem.exists(path)) {
	System.out.println("File does not exists");
	return;
	}	//checking the existence of file
	byte[] b3 = new byte[1024];
	FSDataInputStream read1 = filesystem.open(path);
	 int numBytes = 0;
	try {
	while ((numBytes = read1.read(b3))> 0) {//reading file sequentially 1kb at a time
		}
	}
	catch(Exception e)
	{
		e.printStackTrace();
	}
	finally {
   read1.close();
	}
}

}

