# yahoo-dividends-splits
These Racket programs will download the Yahoo dividends and splits CSV files and insert the data into a PostgreSQL database. The intended usage is:

```bash
$ racket extract.rkt
$ racket transform-load.rkt
```

Both of the above programs will require a database password. The available parameters are:

```bash
$ racket extract.rkt -h
racket extract.rkt [ <option> ... ]
 where <option> is one of
  -c <c>, --cookie <c> : Cookie
  -e <end>, --end-date <end> : Final date for history retrieval. Defaults to today
  -f <first>, --first-symbol <first> : First symbol to query. Defaults to nothing
  -l <last>, --last-symbol <last> : Last symbol to query. Defaults to nothing
  -n <name>, --db-name <name> : Database name. Defaults to 'local'
  -p <password>, --db-pass <password> : Database password
  -r <r>, --crumb <r> : Crumb
  -s <start>, --start-date <start> : Earliest date for history retrieval. Defaults to today
  -u <user>, --db-user <user> : Database user name. Defaults to 'user'
  --help, -h : Show this help
  -- : Do not treat any remaining argument as a switch (at this level)
 Multiple single-letter switches can be combined after one `-'; for
  example: `-h-' is the same as `-h --'

$ racket transform-load.rkt -h
racket transform-load.rkt [ <option> ... ]
 where <option> is one of
  -b <folder>, --base-folder <folder> : Yahoo dividends and splits base folder. Defaults to /var/tmp/yahoo/dividends-splits
  -d <date>, --folder-date <date> : Yahoo dividends and splits folder date. Defaults to today
  -n <name>, --db-name <name> : Database name. Defaults to 'local'
  -p <password>, --db-pass <password> : Database password
  -u <user>, --db-user <user> : Database user name. Defaults to 'user'
  --help, -h : Show this help
  -- : Do not treat any remaining argument as a switch (at this level)
 Multiple single-letter switches can be combined after one `-'; for
  example: `-h-' is the same as `-h --'
```

The provided `schema.sql` file shows the expected schema within the target PostgreSQL instance. This process assumes you can write to a 
`/var/tmp/yahoo/dividends-splits` folder. This process also assumes you have loaded your database with the NASDAQ symbol file information.
This data is provided by the [nasdaq-symbols](https://github.com/evdubs/nasdaq-symbols) project. Despite this project only providing dividends and splits, it is possible to make minor changes to these programs to also download historical data. I have chosen to just get this data from elsewhere (see the [nyse-cta-summary](https://github.com/evdubs/nyse-cta-summary) project) due to the Yahoo data being adjusted for splits, but if you want split and dividend-adjusted data, these programs can hopefully provide a template for doing that.

Finally, there are two parameters required to extract data correctly from Yahoo: cookie and crumb. You can find these values by doing the following:

1. Go to http://finance.yahoo.com in your web browser
2. Load a stock (AAPL will work)
3. Click on the "Historical Data" tab
4. Right-click the "Download Data" link and select "Copy Link Location". This URL will have a query string that includes the crumb field and its value. The value may look something like `aAbBcCdDeE123` and will be what you need to extract.
5. Find the cookies set for this page (in Firefox, right-click, View Page Info, Security, View Cookies). A cookie named "B" will be set. The content of this cookie may look something like `abcdefghi123&b=3&s=ua` and will be what you need to extract.

This cookie and crumb combination should both be valid for a year, so you do not need to do this every time.

### Dependencies

It is recommended that you start with the standard Racket distribution. With that, you will need to install the following packages:

```bash
$ raco pkg install --skip-installed gregor tasks threading
```
