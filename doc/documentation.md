## CWMSPY:  A Simple Python Wrapper for the HEC-CWMS API.

CWMSPY is a wrapper around the HEC-CWMS API built keeping simplicity in mind.

## Guiding principles

- **User Friendly.**  CWMSPY is meant to make CWMS database maintenance easy.  
CWMS methods are a combination of low-level Python wrappers around HEC-CWMS API
functions and procedures and high-level Python methods when procedures or functions
are not user friendly or do not exist.
- **Python over PL/SQL**  CWMSPY aims to allow the majority of database
maintenance to be performed completely in python.  PL/SQL
(Procedural Language for SQL) is Oracle Corporation's procedural extension for
SQL and the Oracle relational database.  It extends Oracle SQL by adding
conditions and loops, but it is complex and lacks the benefits of a dynamic
scripting language.


## Getting Started

Lets start with a simple example:

```python
>>> from cwmspy import CWMS

>>> cwms = CWMS()
>>> cwms.connect()
>>> cwms.connect()
True
>>> df = cwms.retrieve_ts(p_cwms_ts_id='LWG.Flow-Out.Ave.~1Day.1Day.CBT-REV',
                          start_time='2019/1/1', end_time='2019/9/1', df=True)
>>> df.head()
            date_time       value  quality_code
0 2018-12-31 08:00:00  574.831986             0
1 2019-01-01 08:00:00  668.277580             0
2 2019-01-02 08:00:00  608.812202             0
3 2019-01-03 08:00:00  597.485463             0
4 2019-01-04 08:00:00  560.673563             0
```

CWMSPY has multiple ways to connect to the database.  A `.env`
file is provided in the module directory in the above example for quick connection.  

Other methods include:

-  Pass a `cx_Oracle` connection to the CWMS object on instantiation,
--- `c = CWMS(conn=my_connection)`.  
- Pass the `user`, `password`, `service_name`, and `host`, as arguments to the
`connect` method.
--- `cwms.connect(host=my_host, user=my_user, password=my_password, service_name=my_serv)`.

Method parameters that begin with a `p_` are passed directly to a CWMS function or
procedure.  Other parameters either need to be transformed before being passed
or are not passed at all and only used in the method.  

In the above example `p_cwms_ts_id` is a parameter of `CWMS` method `retrieve_ts`
and the `CWMS` Package `Cwms_Ts` procedure `Retrieve_Ts`.  The `start_time` and
`end_time` parameters will be passed to `Retrieve_Ts` after converted to type
`datetime.datetime`, `df` is used in the `retrieve_ts` as an option to return a
`pandas.core.DataFrame`.

That's it.

# Sub-modules

Sub-modules are broken up into different mixin classes with the same naming
convention as CWMS API packages.

# Methods

Methods will be added when time permits and when needed.  Want a new method?  
[Open an issue](https://github.com/jetilton/cwmspy/issues). Or, better yet,
[fork it](https://github.com/login?return_to=%2Fjetilton%2Fcwmspy) and put in a
[pull request](https://github.com/jetilton/cwmspy/pulls).
