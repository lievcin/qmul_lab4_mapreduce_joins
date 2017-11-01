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

			@Override
			protected void setup(Context context) throws IOException, InterruptedException {

				companiesDetails = new Hashtable<String, String>();
				URI fileUri = context.getCacheFiles()[0];

				FileSystem fs = FileSystem.get(context.getConfiguration());
				FSDataInputStream in = fs.open(new Path(fileUri));

				BufferedReader br = new BufferedReader(new InputStreamReader(in));

				String line = null;
				try {
					// we discard the header row
					br.readLine();

					while ((line = br.readLine()) != null) {
						context.getCounter(CustomCounters.NUM_COMPANIES).increment(1);

						String[] fields = line.split("\t");
						// Fields are: 0:Symbol 1:Name 2:IPOyear 3:Sector 4:industry
						if (fields.length == 5)
							companyInfo.put(fields[0], fields[3]);
					}
					br.close();
				} catch (IOException e1) {
				}

				super.setup(context);
			}

	}
