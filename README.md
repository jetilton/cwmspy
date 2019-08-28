# cwmspy
### Wrapper around the [HEC CWMS](https://cwms.usace.army.mil/dokuwiki/doku.php?id=home) api 

Visit the [documentation](https://jetilton.github.io/cwmspy/) for full description.

## Installation

```sh
pip install git+https://github.com/jetilton/cwmspy.git
```

You will need to navigate to the package location and create a .env file with the appropriate user, password, host and service name to automatically connect to the CWMS Oracle database.  You will also need to have followed the instructions for the python package cx_Oracle.

## Usage example
```python
from cwmspy import CWMS 

cwms = CWMS()
cwms.connect()
df = cwms.retrieve_ts('cms','TDA.Flow-Spill.Ave.1Hour.1Hour.CBT-RAW',  '2019/1/1', '2019/9/1', df=True)

df.head()
```
```
            date_time  value  quality_code
0 2018-12-31 23:00:00    NaN             5
1 2019-01-01 00:00:00    0.0             0
2 2019-01-01 01:00:00    0.0             0
3 2019-01-01 02:00:00    0.0             0
4 2019-01-01 03:00:00    0.0             0
```
Visit the [documentation](https://jetilton.github.io/cwmspy/) for more examples.
## Meta

Jeff Tilton – jfftilton@gmail.com

[https://github.com/jetilton/cwmspy](https://github.com/jetilton/)

## Contributing

1. Fork it (<https://github.com/jetilton/cwmspy/fork>)
2. Create your feature branch (`git checkout -b feature/fooBar`)
3. Commit your changes (`git commit -am 'Add some fooBar'`)
4. Push to the branch (`git push origin feature/fooBar`)
5. Create a new Pull Request

<!-- Markdown link & img dfn's -->
[cwms-url]: https://cwms.usace.army.mil/dokuwiki/doku.php?id=home
[travis-image]: https://img.shields.io/travis/dbader/node-datadog-metrics/master.svg?style=flat-square
[travis-url]: https://travis-ci.org/dbader/node-datadog-metrics
