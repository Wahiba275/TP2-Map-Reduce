# TP2-Map-Reduce

```java
 private static final IntWritable ONE = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(" ");
        if(fields.length > 1) {
            String city = fields[1];
            context.write(new Text(city), ONE);
        }
    }
```

```java
private int targetYear = 2021;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(" ");
        if (fields.length == 4) {
            String date = fields[0];
            int year = Integer.parseInt(date.split("-")[0]);
            if (year == targetYear) {
                String city = fields[1];
                String product = fields[2];
                int price = Integer.parseInt(fields[3]);
                Text outputKey = new Text(city + " " + product);
                context.write(outputKey, new IntWritable(price));
            }
        }
    }
```

```java
private static final IntWritable ONE = new IntWritable(1);
 @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(" ");
        if(fields.length > 1) {
            String adress = fields[0];
            context.write(new Text(adress), ONE);
        }
    }
```

```java
@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(" ");
        if(fields.length > 1) {
            int code = Integer.parseInt(fields[6]);
            if (code==200){
                String adress = fields[0];
                context.write(new Text(adress), ONE);
            }
        }
    }
```
