package bdp.stock;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StockSequenceConverterMapper extends Mapper<LongWritable, Text, Text, DailyStock> {

    
    private Text exchange = new Text();
    
    private DailyStock stock = new DailyStock();
    private Text company = new Text();
    private LongWritable day = new LongWritable();
    private DoubleWritable opening = new DoubleWritable();
    private DoubleWritable close = new DoubleWritable();
    private DoubleWritable high = new DoubleWritable();
    private DoubleWritable low = new DoubleWritable();
    private IntWritable volume = new IntWritable();
    private DoubleWritable adjClose = new DoubleWritable();

    public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {

        String[] fields = line.toString().split(",");

        //Fields contains line as follows. 
        //   0         1           2      3    ...........
        //exchange, stock_symbol, date, stock_price_open,stock_price_high,stock_price_low, stock_price_close, stock_volume,stock_price_adj_close.

        // parse key
        // key is the first field, pointing the stock market
       
        // setting day by parsing from
        try {
             exchange.set(fields[0]);

        company.set(fields[1]);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

            day.set(sdf.parse(fields[2]).getTime());

            opening.set(Double.parseDouble(fields[3]));
            close.set(Double.parseDouble(fields[4]));
            high.set(Double.parseDouble(fields[5]));
            low.set(Double.parseDouble(fields[6]));
            volume.set(Integer.parseInt(fields[7]));
            adjClose.set(Double.parseDouble(fields[8]));

            stock.set(company, day, opening, close, high, low, volume,
                    adjClose);

        } catch (ParseException e) {
           
        } catch (NumberFormatException e) {
        }
        //


        context.write(exchange, stock);


    }
}
