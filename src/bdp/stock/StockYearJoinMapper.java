	package bdp.stock;

	import java.io.BufferedReader;
	import java.io.IOException;
	import java.io.InputStreamReader;
	import java.net.URI;
	import java.util.Calendar;
	import java.util.Hashtable;

	import org.apache.hadoop.fs.FSDataInputStream;
	import org.apache.hadoop.fs.FileSystem;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.io.LongWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Mapper;

	public class StockYearJoinMapper extends Mapper<Object, DailyStock, TextIntPair, LongWritable> {

			private Hashtable<String, String> companiesDetails;
			private TextIntPair sectorYear = new TextIntPair();
			private LongWritable dailyVolume = new LongWritable();

	    public void map(Object key, DailyStock entry, Context context) throws IOException, InterruptedException {
	    	String companySymbol = entry.getCompany().toString();
	    	String companySector = companiesDetails.get(companySymbol);

	    	Calendar calendar = Calendar.getInstance();
				calendar.setTimeInMillis(entry.getDay().get());
				int tradingYear = calendar.get(Calendar.YEAR);

				sectorYear.set(companySector, tradingYear);
				dailyVolume.set(entry.getVolume().get());
				context.write(sectorYear, dailyVolume);

	    }
	}
