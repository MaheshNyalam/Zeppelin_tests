%pyspark
from datetime import date,timedelta
import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import *
from datetime import date,timedelta

pc_data = [
    Row(sequence_id=1, cal_year=2000, cal_period=1, start_date=date(2000, 1, 2), end_date=date(2000, 1, 29),days_in_period=28)
    , Row(sequence_id=2, cal_year=2000, cal_period=2, start_date=date(2000, 1, 30), end_date=date(2000, 2, 26),days_in_period=28)
    , Row(sequence_id=3, cal_year=2000, cal_period=3, start_date=date(2000, 2, 27), end_date=date(2000, 3, 25), days_in_period=28)
    , Row(sequence_id=4, cal_year=2000, cal_period=4, start_date=date(2000, 3, 26), end_date=date(2000, 4, 22), days_in_period=28)
    , Row(sequence_id=5, cal_year=2000, cal_period=5, start_date=date(2000, 4, 23), end_date=date(2000, 5, 20), days_in_period=28)
    , Row(sequence_id=6, cal_year=2000, cal_period=6, start_date=date(2000, 5, 21), end_date=date(2000, 6, 17), days_in_period=28)
    , Row(sequence_id=7, cal_year=2000, cal_period=7, start_date=date(2000, 6, 18), end_date=date(2000, 7, 15), days_in_period=28)
    , Row(sequence_id=8, cal_year=2000, cal_period=8, start_date=date(2000, 7, 16), end_date=date(2000, 8, 12), days_in_period=28)
    , Row(sequence_id=9, cal_year=2000, cal_period=9, start_date=date(2000, 8, 13), end_date=date(2000, 9, 9), days_in_period=28)
    , Row(sequence_id=10, cal_year=2000, cal_period=10, start_date=date(2000, 9, 10), end_date=date(2000, 10, 7), days_in_period=28)
    , Row(sequence_id=11, cal_year=2000, cal_period=11, start_date=date(2000, 10, 8), end_date=date(2000, 11, 4), days_in_period=28)
    , Row(sequence_id=12, cal_year=2000, cal_period=12, start_date=date(2000, 11, 5), end_date=date(2000, 12, 2), days_in_period=28)
    , Row(sequence_id=13, cal_year=2000, cal_period=13, start_date=date(2000, 12, 3), end_date=date(2000, 12, 30), days_in_period=28)
    , Row(sequence_id=14, cal_year=2001, cal_period=1, start_date=date(2000, 12, 31), end_date=date(2001, 1, 27), days_in_period=28)
    , Row(sequence_id=15, cal_year=2001, cal_period=2, start_date=date(2001, 1, 28), end_date=date(2001, 2, 24), days_in_period=28)
    , Row(sequence_id=16, cal_year=2001, cal_period=3, start_date=date(2001, 2, 25), end_date=date(2001, 3, 24), days_in_period=28)
    , Row(sequence_id=17, cal_year=2001, cal_period=4, start_date=date(2001, 3, 25), end_date=date(2001, 4, 21), days_in_period=28)
    , Row(sequence_id=18, cal_year=2001, cal_period=5, start_date=date(2001, 4, 22), end_date=date(2001, 5, 19), days_in_period=28)
    , Row(sequence_id=19, cal_year=2001, cal_period=6, start_date=date(2001, 5, 20), end_date=date(2001, 6, 16), days_in_period=28)
    , Row(sequence_id=20, cal_year=2001, cal_period=7, start_date=date(2001, 6, 17), end_date=date(2001, 7, 14), days_in_period=28)
    , Row(sequence_id=21, cal_year=2001, cal_period=8, start_date=date(2001, 7, 15), end_date=date(2001, 8, 11), days_in_period=28)
    , Row(sequence_id=22, cal_year=2001, cal_period=9, start_date=date(2001, 8, 12), end_date=date(2001, 9, 8), days_in_period=28)
    , Row(sequence_id=23, cal_year=2001, cal_period=10, start_date=date(2001, 9, 9), end_date=date(2001, 10, 6), days_in_period=28)
    , Row(sequence_id=24, cal_year=2001, cal_period=11, start_date=date(2001, 10, 7), end_date=date(2001, 11, 3), days_in_period=28)
    , Row(sequence_id=25, cal_year=2001, cal_period=12, start_date=date(2001, 11, 4), end_date=date(2001, 12, 1), days_in_period=28)
    , Row(sequence_id=26, cal_year=2001, cal_period=13, start_date=date(2001, 12, 2), end_date=date(2001, 12, 29), days_in_period=28)
    , Row(sequence_id=27, cal_year=2002, cal_period=1, start_date=date(2001, 12, 30), end_date=date(2002, 1, 26), days_in_period=28)
    , Row(sequence_id=28, cal_year=2002, cal_period=2, start_date=date(2002, 1, 27), end_date=date(2002, 2, 23), days_in_period=28)
    , Row(sequence_id=29, cal_year=2002, cal_period=3, start_date=date(2002, 2, 24), end_date=date(2002, 3, 23), days_in_period=28)
    , Row(sequence_id=30, cal_year=2002, cal_period=4, start_date=date(2002, 3, 24), end_date=date(2002, 4, 20), days_in_period=28)
    , Row(sequence_id=31, cal_year=2002, cal_period=5, start_date=date(2002, 4, 21), end_date=date(2002, 5, 18), days_in_period=28)
    , Row(sequence_id=32, cal_year=2002, cal_period=6, start_date=date(2002, 5, 19), end_date=date(2002, 6, 15), days_in_period=28)
    , Row(sequence_id=33, cal_year=2002, cal_period=7, start_date=date(2002, 6, 16), end_date=date(2002, 7, 13), days_in_period=28)
    , Row(sequence_id=34, cal_year=2002, cal_period=8, start_date=date(2002, 7, 14), end_date=date(2002, 8, 10), days_in_period=28)
    , Row(sequence_id=35, cal_year=2002, cal_period=9, start_date=date(2002, 8, 11), end_date=date(2002, 9, 7), days_in_period=28)
    , Row(sequence_id=36, cal_year=2002, cal_period=10, start_date=date(2002, 9, 8), end_date=date(2002, 10, 5), days_in_period=28)
    , Row(sequence_id=37, cal_year=2002, cal_period=11, start_date=date(2002, 10, 6), end_date=date(2002, 11, 2), days_in_period=28)
    , Row(sequence_id=38, cal_year=2002, cal_period=12, start_date=date(2002, 11, 3), end_date=date(2002, 11, 30), days_in_period=28)
    , Row(sequence_id=39, cal_year=2002, cal_period=13, start_date=date(2002, 12, 1), end_date=date(2002, 12, 28), days_in_period=28)
    , Row(sequence_id=40, cal_year=2003, cal_period=1, start_date=date(2002, 12, 29), end_date=date(2003, 1, 25), days_in_period=28)
    , Row(sequence_id=41, cal_year=2003, cal_period=2, start_date=date(2003, 1, 26), end_date=date(2003, 2, 22), days_in_period=28)
    , Row(sequence_id=42, cal_year=2003, cal_period=3, start_date=date(2003, 2, 23), end_date=date(2003, 3, 22), days_in_period=28)
    , Row(sequence_id=43, cal_year=2003, cal_period=4, start_date=date(2003, 3, 23), end_date=date(2003, 4, 19), days_in_period=28)
    , Row(sequence_id=44, cal_year=2003, cal_period=5, start_date=date(2003, 4, 20), end_date=date(2003, 5, 17), days_in_period=28)
    , Row(sequence_id=45, cal_year=2003, cal_period=6, start_date=date(2003, 5, 18), end_date=date(2003, 6, 14), days_in_period=28)
    , Row(sequence_id=46, cal_year=2003, cal_period=7, start_date=date(2003, 6, 15), end_date=date(2003, 7, 12), days_in_period=28)
    , Row(sequence_id=47, cal_year=2003, cal_period=8, start_date=date(2003, 7, 13), end_date=date(2003, 8, 9), days_in_period=28)
    , Row(sequence_id=48, cal_year=2003, cal_period=9, start_date=date(2003, 8, 10), end_date=date(2003, 9, 6), days_in_period=28)
    , Row(sequence_id=49, cal_year=2003, cal_period=10, start_date=date(2003, 9, 7), end_date=date(2003, 10, 4), days_in_period=28)
    , Row(sequence_id=50, cal_year=2003, cal_period=11, start_date=date(2003, 10, 5), end_date=date(2003, 11, 1), days_in_period=28)
    , Row(sequence_id=51, cal_year=2003, cal_period=12, start_date=date(2003, 11, 2), end_date=date(2003, 11, 29), days_in_period=28)
    , Row(sequence_id=52, cal_year=2003, cal_period=13, start_date=date(2003, 11, 30), end_date=date(2004, 1, 3), days_in_period=35)
    , Row(sequence_id=53, cal_year=2004, cal_period=1, start_date=date(2004, 1, 4), end_date=date(2004, 1, 31), days_in_period=28)
    , Row(sequence_id=54, cal_year=2004, cal_period=2, start_date=date(2004, 2, 1), end_date=date(2004, 2, 28), days_in_period=28)
    , Row(sequence_id=55, cal_year=2004, cal_period=3, start_date=date(2004, 2, 29), end_date=date(2004, 3, 27), days_in_period=28)
    , Row(sequence_id=56, cal_year=2004, cal_period=4, start_date=date(2004, 3, 28), end_date=date(2004, 4, 24), days_in_period=28)
    , Row(sequence_id=57, cal_year=2004, cal_period=5, start_date=date(2004, 4, 25), end_date=date(2004, 5, 22), days_in_period=28)
    , Row(sequence_id=58, cal_year=2004, cal_period=6, start_date=date(2004, 5, 23), end_date=date(2004, 6, 19), days_in_period=28)
    , Row(sequence_id=59, cal_year=2004, cal_period=7, start_date=date(2004, 6, 20), end_date=date(2004, 7, 17), days_in_period=28)
    , Row(sequence_id=60, cal_year=2004, cal_period=8, start_date=date(2004, 7, 18), end_date=date(2004, 8, 14), days_in_period=28)
    , Row(sequence_id=61, cal_year=2004, cal_period=9, start_date=date(2004, 8, 15), end_date=date(2004, 9, 11), days_in_period=28)
    , Row(sequence_id=62, cal_year=2004, cal_period=10, start_date=date(2004, 9, 12), end_date=date(2004, 10, 9), days_in_period=28)
    , Row(sequence_id=63, cal_year=2004, cal_period=11, start_date=date(2004, 10, 10), end_date=date(2004, 11, 6), days_in_period=28)
    , Row(sequence_id=64, cal_year=2004, cal_period=12, start_date=date(2004, 11, 7), end_date=date(2004, 12, 4), days_in_period=28)
    , Row(sequence_id=65, cal_year=2004, cal_period=13, start_date=date(2004, 12, 5), end_date=date(2005, 1, 1), days_in_period=28)
    , Row(sequence_id=66, cal_year=2005, cal_period=1, start_date=date(2005, 1, 2), end_date=date(2005, 1, 29), days_in_period=28)
    , Row(sequence_id=67, cal_year=2005, cal_period=2, start_date=date(2005, 1, 30), end_date=date(2005, 2, 26), days_in_period=28)
    , Row(sequence_id=68, cal_year=2005, cal_period=3, start_date=date(2005, 2, 27), end_date=date(2005, 3, 26), days_in_period=28)
    , Row(sequence_id=69, cal_year=2005, cal_period=4, start_date=date(2005, 3, 27), end_date=date(2005, 4, 23), days_in_period=28)
    , Row(sequence_id=70, cal_year=2005, cal_period=5, start_date=date(2005, 4, 24), end_date=date(2005, 5, 21), days_in_period=28)
    , Row(sequence_id=71, cal_year=2005, cal_period=6, start_date=date(2005, 5, 22), end_date=date(2005, 6, 18), days_in_period=28)
    , Row(sequence_id=72, cal_year=2005, cal_period=7, start_date=date(2005, 6, 19), end_date=date(2005, 7, 16), days_in_period=28)
    , Row(sequence_id=73, cal_year=2005, cal_period=8, start_date=date(2005, 7, 17), end_date=date(2005, 8, 13), days_in_period=28)
    , Row(sequence_id=74, cal_year=2005, cal_period=9, start_date=date(2005, 8, 14), end_date=date(2005, 9, 10), days_in_period=28)
    , Row(sequence_id=75, cal_year=2005, cal_period=10, start_date=date(2005, 9, 11), end_date=date(2005, 10, 8), days_in_period=28)
    , Row(sequence_id=76, cal_year=2005, cal_period=11, start_date=date(2005, 10, 9), end_date=date(2005, 11, 5), days_in_period=28)
    , Row(sequence_id=77, cal_year=2005, cal_period=12, start_date=date(2005, 11, 6), end_date=date(2005, 12, 3), days_in_period=28)
    , Row(sequence_id=78, cal_year=2005, cal_period=13, start_date=date(2005, 12, 4), end_date=date(2005, 12, 31), days_in_period=28)
    , Row(sequence_id=79, cal_year=2006, cal_period=1, start_date=date(2006, 1, 1), end_date=date(2006, 1, 28), days_in_period=28)
    , Row(sequence_id=80, cal_year=2006, cal_period=2, start_date=date(2006, 1, 29), end_date=date(2006, 2, 25), days_in_period=28)
    , Row(sequence_id=81, cal_year=2006, cal_period=3, start_date=date(2006, 2, 26), end_date=date(2006, 3, 25), days_in_period=28)
    , Row(sequence_id=82, cal_year=2006, cal_period=4, start_date=date(2006, 3, 26), end_date=date(2006, 4, 22), days_in_period=28)
    , Row(sequence_id=83, cal_year=2006, cal_period=5, start_date=date(2006, 4, 23), end_date=date(2006, 5, 20), days_in_period=28)
    , Row(sequence_id=84, cal_year=2006, cal_period=6, start_date=date(2006, 5, 21), end_date=date(2006, 6, 17), days_in_period=28)
    , Row(sequence_id=85, cal_year=2006, cal_period=7, start_date=date(2006, 6, 18), end_date=date(2006, 7, 15), days_in_period=28)
    , Row(sequence_id=86, cal_year=2006, cal_period=8, start_date=date(2006, 7, 16), end_date=date(2006, 8, 12), days_in_period=28)
    , Row(sequence_id=87, cal_year=2006, cal_period=9, start_date=date(2006, 8, 13), end_date=date(2006, 9, 9), days_in_period=28)
    , Row(sequence_id=88, cal_year=2006, cal_period=10, start_date=date(2006, 9, 10), end_date=date(2006, 10, 7), days_in_period=28)
    , Row(sequence_id=89, cal_year=2006, cal_period=11, start_date=date(2006, 10, 8), end_date=date(2006, 11, 4), days_in_period=28)
    , Row(sequence_id=90, cal_year=2006, cal_period=12, start_date=date(2006, 11, 5), end_date=date(2006, 12, 2), days_in_period=28)
    , Row(sequence_id=91, cal_year=2006, cal_period=13, start_date=date(2006, 12, 3), end_date=date(2006, 12, 30), days_in_period=28)
    , Row(sequence_id=92, cal_year=2007, cal_period=1, start_date=date(2006, 12, 31), end_date=date(2007, 1, 27), days_in_period=28)
    , Row(sequence_id=93, cal_year=2007, cal_period=2, start_date=date(2007, 1, 28), end_date=date(2007, 2, 24), days_in_period=28)
    , Row(sequence_id=94, cal_year=2007, cal_period=3, start_date=date(2007, 2, 25), end_date=date(2007, 3, 24), days_in_period=28)
    , Row(sequence_id=95, cal_year=2007, cal_period=4, start_date=date(2007, 3, 25), end_date=date(2007, 4, 21), days_in_period=28)
    , Row(sequence_id=96, cal_year=2007, cal_period=5, start_date=date(2007, 4, 22), end_date=date(2007, 5, 19), days_in_period=28)
    , Row(sequence_id=97, cal_year=2007, cal_period=6, start_date=date(2007, 5, 20), end_date=date(2007, 6, 16), days_in_period=28)
    , Row(sequence_id=98, cal_year=2007, cal_period=7, start_date=date(2007, 6, 17), end_date=date(2007, 7, 14), days_in_period=28)
    , Row(sequence_id=99, cal_year=2007, cal_period=8, start_date=date(2007, 7, 15), end_date=date(2007, 8, 11), days_in_period=28)
    , Row(sequence_id=100, cal_year=2007, cal_period=9, start_date=date(2007, 8, 12), end_date=date(2007, 9, 8), days_in_period=28)
    , Row(sequence_id=101, cal_year=2007, cal_period=10, start_date=date(2007, 9, 9), end_date=date(2007, 10, 6), days_in_period=28)
    , Row(sequence_id=102, cal_year=2007, cal_period=11, start_date=date(2007, 10, 7), end_date=date(2007, 11, 3), days_in_period=28)
    , Row(sequence_id=103, cal_year=2007, cal_period=12, start_date=date(2007, 11, 4), end_date=date(2007, 12, 1), days_in_period=28)
    , Row(sequence_id=104, cal_year=2007, cal_period=13, start_date=date(2007, 12, 2), end_date=date(2007, 12, 29), days_in_period=28)
    , Row(sequence_id=105, cal_year=2008, cal_period=1, start_date=date(2007, 12, 30), end_date=date(2008, 1, 26), days_in_period=28)
    , Row(sequence_id=106, cal_year=2008, cal_period=2, start_date=date(2008, 1, 27), end_date=date(2008, 2, 23), days_in_period=28)
    , Row(sequence_id=107, cal_year=2008, cal_period=3, start_date=date(2008, 2, 24), end_date=date(2008, 3, 22), days_in_period=28)
    , Row(sequence_id=108, cal_year=2008, cal_period=4, start_date=date(2008, 3, 23), end_date=date(2008, 4, 19), days_in_period=28)
    , Row(sequence_id=109, cal_year=2008, cal_period=5, start_date=date(2008, 4, 20), end_date=date(2008, 5, 17), days_in_period=28)
    , Row(sequence_id=110, cal_year=2008, cal_period=6, start_date=date(2008, 5, 18), end_date=date(2008, 6, 14), days_in_period=28)
    , Row(sequence_id=111, cal_year=2008, cal_period=7, start_date=date(2008, 6, 15), end_date=date(2008, 7, 12), days_in_period=28)
    , Row(sequence_id=112, cal_year=2008, cal_period=8, start_date=date(2008, 7, 13), end_date=date(2008, 8, 9), days_in_period=28)
    , Row(sequence_id=113, cal_year=2008, cal_period=9, start_date=date(2008, 8, 10), end_date=date(2008, 9, 6), days_in_period=28)
    , Row(sequence_id=114, cal_year=2008, cal_period=10, start_date=date(2008, 9, 7), end_date=date(2008, 10, 4), days_in_period=28)
    , Row(sequence_id=115, cal_year=2008, cal_period=11, start_date=date(2008, 10, 5), end_date=date(2008, 11, 1), days_in_period=28)
    , Row(sequence_id=116, cal_year=2008, cal_period=12, start_date=date(2008, 11, 2), end_date=date(2008, 11, 29), days_in_period=28)
    , Row(sequence_id=117, cal_year=2008, cal_period=13, start_date=date(2008, 11, 30), end_date=date(2009, 1, 3), days_in_period=35)
    , Row(sequence_id=118, cal_year=2009, cal_period=1, start_date=date(2009, 1, 4), end_date=date(2009, 1, 31), days_in_period=28)
    , Row(sequence_id=119, cal_year=2009, cal_period=2, start_date=date(2009, 2, 1), end_date=date(2009, 2, 28), days_in_period=28)
    , Row(sequence_id=120, cal_year=2009, cal_period=3, start_date=date(2009, 3, 1), end_date=date(2009, 3, 28), days_in_period=28)
    , Row(sequence_id=121, cal_year=2009, cal_period=4, start_date=date(2009, 3, 29), end_date=date(2009, 4, 25), days_in_period=28)
    , Row(sequence_id=122, cal_year=2009, cal_period=5, start_date=date(2009, 4, 26), end_date=date(2009, 5, 23), days_in_period=28)
    , Row(sequence_id=123, cal_year=2009, cal_period=6, start_date=date(2009, 5, 24), end_date=date(2009, 6, 20), days_in_period=28)
    , Row(sequence_id=124, cal_year=2009, cal_period=7, start_date=date(2009, 6, 21), end_date=date(2009, 7, 18), days_in_period=28)
    , Row(sequence_id=125, cal_year=2009, cal_period=8, start_date=date(2009, 7, 19), end_date=date(2009, 8, 15), days_in_period=28)
    , Row(sequence_id=126, cal_year=2009, cal_period=9, start_date=date(2009, 8, 16), end_date=date(2009, 9, 12), days_in_period=28)
    , Row(sequence_id=127, cal_year=2009, cal_period=10, start_date=date(2009, 9, 13), end_date=date(2009, 10, 10), days_in_period=28)
    , Row(sequence_id=128, cal_year=2009, cal_period=11, start_date=date(2009, 10, 11), end_date=date(2009, 11, 7), days_in_period=28)
    , Row(sequence_id=129, cal_year=2009, cal_period=12, start_date=date(2009, 11, 8), end_date=date(2009, 12, 5), days_in_period=28)
    , Row(sequence_id=130, cal_year=2009, cal_period=13, start_date=date(2009, 12, 6), end_date=date(2010, 1, 2), days_in_period=28)
    , Row(sequence_id=131, cal_year=2010, cal_period=1, start_date=date(2010, 1, 3), end_date=date(2010, 1, 30), days_in_period=28)
    , Row(sequence_id=132, cal_year=2010, cal_period=2, start_date=date(2010, 1, 31), end_date=date(2010, 2, 27), days_in_period=28)
    , Row(sequence_id=133, cal_year=2010, cal_period=3, start_date=date(2010, 2, 28), end_date=date(2010, 3, 27), days_in_period=28)
    , Row(sequence_id=134, cal_year=2010, cal_period=4, start_date=date(2010, 3, 28), end_date=date(2010, 4, 24), days_in_period=28)
    , Row(sequence_id=135, cal_year=2010, cal_period=5, start_date=date(2010, 4, 25), end_date=date(2010, 5, 22), days_in_period=28)
    , Row(sequence_id=136, cal_year=2010, cal_period=6, start_date=date(2010, 5, 23), end_date=date(2010, 6, 19), days_in_period=28)
    , Row(sequence_id=137, cal_year=2010, cal_period=7, start_date=date(2010, 6, 20), end_date=date(2010, 7, 17), days_in_period=28)
    , Row(sequence_id=138, cal_year=2010, cal_period=8, start_date=date(2010, 7, 18), end_date=date(2010, 8, 14), days_in_period=28)
    , Row(sequence_id=139, cal_year=2010, cal_period=9, start_date=date(2010, 8, 15), end_date=date(2010, 9, 11), days_in_period=28)
    , Row(sequence_id=140, cal_year=2010, cal_period=10, start_date=date(2010, 9, 12), end_date=date(2010, 10, 9), days_in_period=28)
    , Row(sequence_id=141, cal_year=2010, cal_period=11, start_date=date(2010, 10, 10), end_date=date(2010, 11, 6), days_in_period=28)
    , Row(sequence_id=142, cal_year=2010, cal_period=12, start_date=date(2010, 11, 7), end_date=date(2010, 12, 4), days_in_period=28)
    , Row(sequence_id=143, cal_year=2010, cal_period=13, start_date=date(2010, 12, 5), end_date=date(2011, 1, 1), days_in_period=28)
    , Row(sequence_id=144, cal_year=2011, cal_period=1, start_date=date(2011, 1, 2), end_date=date(2011, 1, 29), days_in_period=28)
    , Row(sequence_id=145, cal_year=2011, cal_period=2, start_date=date(2011, 1, 30), end_date=date(2011, 2, 26), days_in_period=28)
    , Row(sequence_id=146, cal_year=2011, cal_period=3, start_date=date(2011, 2, 27), end_date=date(2011, 3, 26), days_in_period=28)
    , Row(sequence_id=147, cal_year=2011, cal_period=4, start_date=date(2011, 3, 27), end_date=date(2011, 4, 23), days_in_period=28)
    , Row(sequence_id=148, cal_year=2011, cal_period=5, start_date=date(2011, 4, 24), end_date=date(2011, 5, 21), days_in_period=28)
    , Row(sequence_id=149, cal_year=2011, cal_period=6, start_date=date(2011, 5, 22), end_date=date(2011, 6, 18), days_in_period=28)
    , Row(sequence_id=150, cal_year=2011, cal_period=7, start_date=date(2011, 6, 19), end_date=date(2011, 7, 16), days_in_period=28)
    , Row(sequence_id=151, cal_year=2011, cal_period=8, start_date=date(2011, 7, 17), end_date=date(2011, 8, 13), days_in_period=28)
    , Row(sequence_id=152, cal_year=2011, cal_period=9, start_date=date(2011, 8, 14), end_date=date(2011, 9, 10), days_in_period=28)
    , Row(sequence_id=153, cal_year=2011, cal_period=10, start_date=date(2011, 9, 11), end_date=date(2011, 10, 8), days_in_period=28)
    , Row(sequence_id=154, cal_year=2011, cal_period=11, start_date=date(2011, 10, 9), end_date=date(2011, 11, 5), days_in_period=28)
    , Row(sequence_id=155, cal_year=2011, cal_period=12, start_date=date(2011, 11, 6), end_date=date(2011, 12, 3), days_in_period=28)
    , Row(sequence_id=156, cal_year=2011, cal_period=13, start_date=date(2011, 12, 4), end_date=date(2011, 12, 31), days_in_period=28)
    , Row(sequence_id=157, cal_year=2012, cal_period=1, start_date=date(2012, 1, 1), end_date=date(2012, 1, 28), days_in_period=28)
    , Row(sequence_id=158, cal_year=2012, cal_period=2, start_date=date(2012, 1, 29), end_date=date(2012, 2, 25), days_in_period=28)
    , Row(sequence_id=159, cal_year=2012, cal_period=3, start_date=date(2012, 2, 26), end_date=date(2012, 3, 24), days_in_period=28)
    , Row(sequence_id=160, cal_year=2012, cal_period=4, start_date=date(2012, 3, 25), end_date=date(2012, 4, 21), days_in_period=28)
    , Row(sequence_id=161, cal_year=2012, cal_period=5, start_date=date(2012, 4, 22), end_date=date(2012, 5, 19), days_in_period=28)
    , Row(sequence_id=162, cal_year=2012, cal_period=6, start_date=date(2012, 5, 20), end_date=date(2012, 6, 16), days_in_period=28)
    , Row(sequence_id=163, cal_year=2012, cal_period=7, start_date=date(2012, 6, 17), end_date=date(2012, 7, 14), days_in_period=28)
    , Row(sequence_id=164, cal_year=2012, cal_period=8, start_date=date(2012, 7, 15), end_date=date(2012, 8, 11), days_in_period=28)
    , Row(sequence_id=165, cal_year=2012, cal_period=9, start_date=date(2012, 8, 12), end_date=date(2012, 9, 8), days_in_period=28)
    , Row(sequence_id=166, cal_year=2012, cal_period=10, start_date=date(2012, 9, 9), end_date=date(2012, 10, 6), days_in_period=28)
    , Row(sequence_id=167, cal_year=2012, cal_period=11, start_date=date(2012, 10, 7), end_date=date(2012, 11, 3), days_in_period=28)
    , Row(sequence_id=168, cal_year=2012, cal_period=12, start_date=date(2012, 11, 4), end_date=date(2012, 12, 1), days_in_period=28)
    , Row(sequence_id=169, cal_year=2012, cal_period=13, start_date=date(2012, 12, 2), end_date=date(2012, 12, 29), days_in_period=28)
    , Row(sequence_id=170, cal_year=2013, cal_period=1, start_date=date(2012, 12, 30), end_date=date(2013, 1, 26), days_in_period=28)
    , Row(sequence_id=171, cal_year=2013, cal_period=2, start_date=date(2013, 1, 27), end_date=date(2013, 2, 23), days_in_period=28)
    , Row(sequence_id=172, cal_year=2013, cal_period=3, start_date=date(2013, 2, 24), end_date=date(2013, 3, 23), days_in_period=28)
    , Row(sequence_id=173, cal_year=2013, cal_period=4, start_date=date(2013, 3, 24), end_date=date(2013, 4, 20), days_in_period=28)
    , Row(sequence_id=174, cal_year=2013, cal_period=5, start_date=date(2013, 4, 21), end_date=date(2013, 5, 18), days_in_period=28)
    , Row(sequence_id=175, cal_year=2013, cal_period=6, start_date=date(2013, 5, 19), end_date=date(2013, 6, 15), days_in_period=28)
    , Row(sequence_id=176, cal_year=2013, cal_period=7, start_date=date(2013, 6, 16), end_date=date(2013, 7, 13), days_in_period=28)
    , Row(sequence_id=177, cal_year=2013, cal_period=8, start_date=date(2013, 7, 14), end_date=date(2013, 8, 10), days_in_period=28)
    , Row(sequence_id=178, cal_year=2013, cal_period=9, start_date=date(2013, 8, 11), end_date=date(2013, 9, 7), days_in_period=28)
    , Row(sequence_id=179, cal_year=2013, cal_period=10, start_date=date(2013, 9, 8), end_date=date(2013, 10, 5), days_in_period=28)
    , Row(sequence_id=180, cal_year=2013, cal_period=11, start_date=date(2013, 10, 6), end_date=date(2013, 11, 2), days_in_period=28)
    , Row(sequence_id=181, cal_year=2013, cal_period=12, start_date=date(2013, 11, 3), end_date=date(2013, 11, 30), days_in_period=28)
    , Row(sequence_id=182, cal_year=2013, cal_period=13, start_date=date(2013, 12, 1), end_date=date(2013, 12, 28), days_in_period=28)
    , Row(sequence_id=183, cal_year=2014, cal_period=1, start_date=date(2013, 12, 29), end_date=date(2014, 1, 25), days_in_period=28)
    , Row(sequence_id=184, cal_year=2014, cal_period=2, start_date=date(2014, 1, 26), end_date=date(2014, 2, 22), days_in_period=28)
    , Row(sequence_id=185, cal_year=2014, cal_period=3, start_date=date(2014, 2, 23), end_date=date(2014, 3, 22), days_in_period=28)
    , Row(sequence_id=186, cal_year=2014, cal_period=4, start_date=date(2014, 3, 23), end_date=date(2014, 4, 19), days_in_period=28)
    , Row(sequence_id=187, cal_year=2014, cal_period=5, start_date=date(2014, 4, 20), end_date=date(2014, 5, 17), days_in_period=28)
    , Row(sequence_id=188, cal_year=2014, cal_period=6, start_date=date(2014, 5, 18), end_date=date(2014, 6, 14), days_in_period=28)
    , Row(sequence_id=189, cal_year=2014, cal_period=7, start_date=date(2014, 6, 15), end_date=date(2014, 7, 12), days_in_period=28)
    , Row(sequence_id=190, cal_year=2014, cal_period=8, start_date=date(2014, 7, 13), end_date=date(2014, 8, 9), days_in_period=28)
    , Row(sequence_id=191, cal_year=2014, cal_period=9, start_date=date(2014, 8, 10), end_date=date(2014, 9, 6), days_in_period=28)
    , Row(sequence_id=192, cal_year=2014, cal_period=10, start_date=date(2014, 9, 7), end_date=date(2014, 10, 4), days_in_period=28)
    , Row(sequence_id=193, cal_year=2014, cal_period=11, start_date=date(2014, 10, 5), end_date=date(2014, 11, 1), days_in_period=28)
    , Row(sequence_id=194, cal_year=2014, cal_period=12, start_date=date(2014, 11, 2), end_date=date(2014, 11, 29), days_in_period=28)
    , Row(sequence_id=195, cal_year=2014, cal_period=13, start_date=date(2014, 11, 30), end_date=date(2015, 1, 3), days_in_period=35)
    , Row(sequence_id=196, cal_year=2015, cal_period=1, start_date=date(2015, 1, 4), end_date=date(2015, 1, 31), days_in_period=28)
    , Row(sequence_id=197, cal_year=2015, cal_period=2, start_date=date(2015, 2, 1), end_date=date(2015, 2, 28), days_in_period=28)
    , Row(sequence_id=198, cal_year=2015, cal_period=3, start_date=date(2015, 3, 1), end_date=date(2015, 3, 28), days_in_period=28)
    , Row(sequence_id=199, cal_year=2015, cal_period=4, start_date=date(2015, 3, 29), end_date=date(2015, 4, 25), days_in_period=28)
    , Row(sequence_id=200, cal_year=2015, cal_period=5, start_date=date(2015, 4, 26), end_date=date(2015, 5, 23), days_in_period=28)
    , Row(sequence_id=201, cal_year=2015, cal_period=6, start_date=date(2015, 5, 24), end_date=date(2015, 6, 20), days_in_period=28)
    , Row(sequence_id=202, cal_year=2015, cal_period=7, start_date=date(2015, 6, 21), end_date=date(2015, 7, 18), days_in_period=28)
    , Row(sequence_id=203, cal_year=2015, cal_period=8, start_date=date(2015, 7, 19), end_date=date(2015, 8, 15), days_in_period=28)
    , Row(sequence_id=204, cal_year=2015, cal_period=9, start_date=date(2015, 8, 16), end_date=date(2015, 9, 12), days_in_period=28)
    , Row(sequence_id=205, cal_year=2015, cal_period=10, start_date=date(2015, 9, 13), end_date=date(2015, 10, 10), days_in_period=28)
    , Row(sequence_id=206, cal_year=2015, cal_period=11, start_date=date(2015, 10, 11), end_date=date(2015, 11, 7), days_in_period=28)
    , Row(sequence_id=207, cal_year=2015, cal_period=12, start_date=date(2015, 11, 8), end_date=date(2015, 12, 5), days_in_period=28)
    , Row(sequence_id=208, cal_year=2015, cal_period=13, start_date=date(2015, 12, 6), end_date=date(2016, 1, 2), days_in_period=28)
    , Row(sequence_id=209, cal_year=2016, cal_period=1, start_date=date(2016, 1, 3), end_date=date(2016, 1, 30), days_in_period=28)
    , Row(sequence_id=210, cal_year=2016, cal_period=2, start_date=date(2016, 1, 31), end_date=date(2016, 2, 27), days_in_period=28)
    , Row(sequence_id=211, cal_year=2016, cal_period=3, start_date=date(2016, 2, 28), end_date=date(2016, 3, 26), days_in_period=28)
    , Row(sequence_id=212, cal_year=2016, cal_period=4, start_date=date(2016, 3, 27), end_date=date(2016, 4, 23), days_in_period=28)
    , Row(sequence_id=213, cal_year=2016, cal_period=5, start_date=date(2016, 4, 24), end_date=date(2016, 5, 21), days_in_period=28)
    , Row(sequence_id=214, cal_year=2016, cal_period=6, start_date=date(2016, 5, 22), end_date=date(2016, 6, 18), days_in_period=28)
    , Row(sequence_id=215, cal_year=2016, cal_period=7, start_date=date(2016, 6, 19), end_date=date(2016, 7, 16), days_in_period=28)
    , Row(sequence_id=216, cal_year=2016, cal_period=8, start_date=date(2016, 7, 17), end_date=date(2016, 8, 13), days_in_period=28)
    , Row(sequence_id=217, cal_year=2016, cal_period=9, start_date=date(2016, 8, 14), end_date=date(2016, 9, 10), days_in_period=28)
    , Row(sequence_id=218, cal_year=2016, cal_period=10, start_date=date(2016, 9, 11), end_date=date(2016, 10, 8), days_in_period=28)
    , Row(sequence_id=219, cal_year=2016, cal_period=11, start_date=date(2016, 10, 9), end_date=date(2016, 11, 5), days_in_period=28)
    , Row(sequence_id=220, cal_year=2016, cal_period=12, start_date=date(2016, 11, 6), end_date=date(2016, 12, 3), days_in_period=28)
    , Row(sequence_id=221, cal_year=2016, cal_period=13, start_date=date(2016, 12, 4), end_date=date(2016, 12, 31), days_in_period=28)
    , Row(sequence_id=222, cal_year=2017, cal_period=1, start_date=date(2017, 1, 1), end_date=date(2017, 1, 28), days_in_period=28)
    , Row(sequence_id=223, cal_year=2017, cal_period=2, start_date=date(2017, 1, 29), end_date=date(2017, 2, 25), days_in_period=28)
    , Row(sequence_id=224, cal_year=2017, cal_period=3, start_date=date(2017, 2, 26), end_date=date(2017, 3, 25), days_in_period=28)
    , Row(sequence_id=225, cal_year=2017, cal_period=4, start_date=date(2017, 3, 26), end_date=date(2017, 4, 22), days_in_period=28)
    , Row(sequence_id=226, cal_year=2017, cal_period=5, start_date=date(2017, 4, 23), end_date=date(2017, 5, 20), days_in_period=28)
    , Row(sequence_id=227, cal_year=2017, cal_period=6, start_date=date(2017, 5, 21), end_date=date(2017, 6, 17), days_in_period=28)
    , Row(sequence_id=228, cal_year=2017, cal_period=7, start_date=date(2017, 6, 18), end_date=date(2017, 7, 15), days_in_period=28)
    , Row(sequence_id=229, cal_year=2017, cal_period=8, start_date=date(2017, 7, 16), end_date=date(2017, 8, 12), days_in_period=28)
    , Row(sequence_id=230, cal_year=2017, cal_period=9, start_date=date(2017, 8, 13), end_date=date(2017, 9, 9), days_in_period=28)
    , Row(sequence_id=231, cal_year=2017, cal_period=10, start_date=date(2017, 9, 10), end_date=date(2017, 10, 7), days_in_period=28)
    , Row(sequence_id=232, cal_year=2017, cal_period=11, start_date=date(2017, 10, 8), end_date=date(2017, 11, 4), days_in_period=28)
    , Row(sequence_id=233, cal_year=2017, cal_period=12, start_date=date(2017, 11, 5), end_date=date(2017, 12, 2), days_in_period=28)
    , Row(sequence_id=234, cal_year=2017, cal_period=13, start_date=date(2017, 12, 3), end_date=date(2017, 12, 30), days_in_period=28)
    , Row(sequence_id=235, cal_year=2018, cal_period=1, start_date=date(2017, 12, 31), end_date=date(2018, 1, 27), days_in_period=28)
    , Row(sequence_id=236, cal_year=2018, cal_period=2, start_date=date(2018, 1, 28), end_date=date(2018, 2, 24), days_in_period=28)
    , Row(sequence_id=237, cal_year=2018, cal_period=3, start_date=date(2018, 2, 25), end_date=date(2018, 3, 24), days_in_period=28)
    , Row(sequence_id=238, cal_year=2018, cal_period=4, start_date=date(2018, 3, 25), end_date=date(2018, 4, 21), days_in_period=28)
    , Row(sequence_id=239, cal_year=2018, cal_period=5, start_date=date(2018, 4, 22), end_date=date(2018, 5, 19), days_in_period=28)
    , Row(sequence_id=240, cal_year=2018, cal_period=6, start_date=date(2018, 5, 20), end_date=date(2018, 6, 16), days_in_period=28)
    , Row(sequence_id=241, cal_year=2018, cal_period=7, start_date=date(2018, 6, 17), end_date=date(2018, 7, 14), days_in_period=28)
    , Row(sequence_id=242, cal_year=2018, cal_period=8, start_date=date(2018, 7, 15), end_date=date(2018, 8, 11), days_in_period=28)
    , Row(sequence_id=243, cal_year=2018, cal_period=9, start_date=date(2018, 8, 12), end_date=date(2018, 9, 8), days_in_period=28)
    , Row(sequence_id=244, cal_year=2018, cal_period=10, start_date=date(2018, 9, 9), end_date=date(2018, 10, 6), days_in_period=28)
    , Row(sequence_id=245, cal_year=2018, cal_period=11, start_date=date(2018, 10, 7), end_date=date(2018, 11, 3), days_in_period=28)
    , Row(sequence_id=246, cal_year=2018, cal_period=12, start_date=date(2018, 11, 4), end_date=date(2018, 12, 1), days_in_period=28)
    , Row(sequence_id=247, cal_year=2018, cal_period=13, start_date=date(2018, 12, 2), end_date=date(2018, 12, 29), days_in_period=28)
    , Row(sequence_id=248, cal_year=2019, cal_period=1, start_date=date(2018, 12, 30), end_date=date(2019, 1, 26), days_in_period=28)
    , Row(sequence_id=249, cal_year=2019, cal_period=2, start_date=date(2019, 1, 27), end_date=date(2019, 2, 23), days_in_period=28)
    , Row(sequence_id=250, cal_year=2019, cal_period=3, start_date=date(2019, 2, 24), end_date=date(2019, 3, 23), days_in_period=28)
    , Row(sequence_id=251, cal_year=2019, cal_period=4, start_date=date(2019, 3, 24), end_date=date(2019, 4, 20), days_in_period=28)
    , Row(sequence_id=252, cal_year=2019, cal_period=5, start_date=date(2019, 4, 21), end_date=date(2019, 5, 18), days_in_period=28)
    , Row(sequence_id=253, cal_year=2019, cal_period=6, start_date=date(2019, 5, 19), end_date=date(2019, 6, 15), days_in_period=28)
    , Row(sequence_id=254, cal_year=2019, cal_period=7, start_date=date(2019, 6, 16), end_date=date(2019, 7, 13), days_in_period=28)
    , Row(sequence_id=255, cal_year=2019, cal_period=8, start_date=date(2019, 7, 14), end_date=date(2019, 8, 10), days_in_period=28)
    , Row(sequence_id=256, cal_year=2019, cal_period=9, start_date=date(2019, 8, 11), end_date=date(2019, 9, 7), days_in_period=28)
    , Row(sequence_id=257, cal_year=2019, cal_period=10, start_date=date(2019, 9, 8), end_date=date(2019, 10, 5), days_in_period=28)
    , Row(sequence_id=258, cal_year=2019, cal_period=11, start_date=date(2019, 10, 6), end_date=date(2019, 11, 2), days_in_period=28)
    , Row(sequence_id=259, cal_year=2019, cal_period=12, start_date=date(2019, 11, 3), end_date=date(2019, 11, 30), days_in_period=28)
    , Row(sequence_id=260, cal_year=2019, cal_period=13, start_date=date(2019, 12, 1), end_date=date(2019, 12, 28), days_in_period=28)
    , Row(sequence_id=261, cal_year=2020, cal_period=1, start_date=date(2019, 12, 29), end_date=date(2020, 1, 25), days_in_period=28)
    , Row(sequence_id=262, cal_year=2020, cal_period=2, start_date=date(2020, 1, 26), end_date=date(2020, 2, 22), days_in_period=28)
    , Row(sequence_id=263, cal_year=2020, cal_period=3, start_date=date(2020, 2, 23), end_date=date(2020, 3, 21), days_in_period=28)
    , Row(sequence_id=264, cal_year=2020, cal_period=4, start_date=date(2020, 3, 22), end_date=date(2020, 4, 18), days_in_period=28)
    , Row(sequence_id=265, cal_year=2020, cal_period=5, start_date=date(2020, 4, 19), end_date=date(2020, 5, 16), days_in_period=28)
    , Row(sequence_id=266, cal_year=2020, cal_period=6, start_date=date(2020, 5, 17), end_date=date(2020, 6, 13), days_in_period=28)
    , Row(sequence_id=267, cal_year=2020, cal_period=7, start_date=date(2020, 6, 14), end_date=date(2020, 7, 11), days_in_period=28)
    , Row(sequence_id=268, cal_year=2020, cal_period=8, start_date=date(2020, 7, 12), end_date=date(2020, 8, 8), days_in_period=28)
    , Row(sequence_id=269, cal_year=2020, cal_period=9, start_date=date(2020, 8, 9), end_date=date(2020, 9, 5), days_in_period=28)
    , Row(sequence_id=270, cal_year=2020, cal_period=10, start_date=date(2020, 9, 6), end_date=date(2020, 10, 3), days_in_period=28)
    , Row(sequence_id=271, cal_year=2020, cal_period=11, start_date=date(2020, 10, 4), end_date=date(2020, 10, 31), days_in_period=28)
    , Row(sequence_id=272, cal_year=2020, cal_period=12, start_date=date(2020, 11, 1), end_date=date(2020, 11, 28), days_in_period=28)
    , Row(sequence_id=273, cal_year=2020, cal_period=13, start_date=date(2020, 11, 29), end_date=date(2021, 1, 2), days_in_period=35)
    , Row(sequence_id=274, cal_year=2021, cal_period=1, start_date=date(2021, 1, 3), end_date=date(2021, 1, 30), days_in_period=28)
    , Row(sequence_id=275, cal_year=2021, cal_period=2, start_date=date(2021, 1, 31), end_date=date(2021, 2, 27), days_in_period=28)
    , Row(sequence_id=276, cal_year=2021, cal_period=3, start_date=date(2021, 2, 28), end_date=date(2021, 3, 27), days_in_period=28)
    , Row(sequence_id=277, cal_year=2021, cal_period=4, start_date=date(2021, 3, 28), end_date=date(2021, 4, 24), days_in_period=28)
    , Row(sequence_id=278, cal_year=2021, cal_period=5, start_date=date(2021, 4, 25), end_date=date(2021, 5, 22), days_in_period=28)
    , Row(sequence_id=279, cal_year=2021, cal_period=6, start_date=date(2021, 5, 23), end_date=date(2021, 6, 19), days_in_period=28)
    , Row(sequence_id=280, cal_year=2021, cal_period=7, start_date=date(2021, 6, 20), end_date=date(2021, 7, 17), days_in_period=28)
    , Row(sequence_id=281, cal_year=2021, cal_period=8, start_date=date(2021, 7, 18), end_date=date(2021, 8, 14), days_in_period=28)
    , Row(sequence_id=282, cal_year=2021, cal_period=9, start_date=date(2021, 8, 15), end_date=date(2021, 9, 11), days_in_period=28)
    , Row(sequence_id=283, cal_year=2021, cal_period=10, start_date=date(2021, 9, 12), end_date=date(2021, 10, 9), days_in_period=28)
    , Row(sequence_id=284, cal_year=2021, cal_period=11, start_date=date(2021, 10, 10), end_date=date(2021, 11, 6), days_in_period=28)
    , Row(sequence_id=285, cal_year=2021, cal_period=12, start_date=date(2021, 11, 7), end_date=date(2021, 12, 4), days_in_period=28)
    , Row(sequence_id=286, cal_year=2021, cal_period=13, start_date=date(2021, 12, 5), end_date=date(2022, 1, 1), days_in_period=28)

]

pc_df = None

def init_pc_df(sqlContext):

    global pc_df

    if not pc_df:
        pc_df = sqlContext.createDataFrame(pc_data)
        pc_df.cache()
    else:
        pass

    return pc_df

# API to get the period calendar data frame
def get_pc_df():

    global pc_df

    return pc_df

# Get the financial period info for a given date
def fp_info(cdate):
    return  get_pc_df().filter((cdate >= pc_df.start_date) & (cdate <= pc_df.end_date)).first()

# Get the start and end period info, nPeriods prior, relative to cdate
# First previous period is the full perior prior to current period
def fps_start_end_info(cdate, nPeriods):
    current_period_info = fp_info(cdate)
    # If cdate is the last day of the period, include that and go back nPeriods -1
    # If not, go back nPeriods.
    start_period_sequence_id = current_period_info.sequence_id - nPeriods + 1
    end_period_sequence_id = current_period_info.sequence_id;
    end_period_info = None
    if (current_period_info.end_date != cdate):
        start_period_sequence_id -= 1
        end_period_sequence_id -= 1
        #end_period_info = current_period_info
    else:
        #end_period_sequence_id -= 1
        pass
    start_period_info = get_pc_df().filter(pc_df.sequence_id == start_period_sequence_id).first()

    if not end_period_info:
        end_period_info = get_pc_df().filter(pc_df.sequence_id == end_period_sequence_id).first()

    x=start_period_info.start_date
    #print 'hello'
    #print x

    #y = Row(start_period_info= start_period_info, end_period_info= end_period_info)
    #print y
    return  Row(start_period_info= start_period_info, end_period_info= end_period_info)



def get_previous_year_start_date(ref_date, years=1):
    last_year_date = datetime.date(ref_date.year, ref_date.month, ref_date.day) + timedelta(days=-365)
    return last_year_date + datetime.timedelta(days=1)

   

%pyspark
from datetime import date,timedelta
import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import *
pc_data = [
    Row(sequence_id=1, cal_year=2000, cal_period=1, start_date=date(2000, 1, 2), end_date=date(2000, 1, 29),days_in_period=28)
    , Row(sequence_id=2, cal_year=2000, cal_period=2, start_date=date(2000, 1, 30), end_date=date(2000, 2, 26),days_in_period=28)
    , Row(sequence_id=3, cal_year=2000, cal_period=3, start_date=date(2000, 2, 27), end_date=date(2000, 3, 25), days_in_period=28)
    , Row(sequence_id=4, cal_year=2000, cal_period=4, start_date=date(2000, 3, 26), end_date=date(2000, 4, 22), days_in_period=28)
    , Row(sequence_id=5, cal_year=2000, cal_period=5, start_date=date(2000, 4, 23), end_date=date(2000, 5, 20), days_in_period=28)
    , Row(sequence_id=6, cal_year=2000, cal_period=6, start_date=date(2000, 5, 21), end_date=date(2000, 6, 17), days_in_period=28)
    , Row(sequence_id=7, cal_year=2000, cal_period=7, start_date=date(2000, 6, 18), end_date=date(2000, 7, 15), days_in_period=28)
    , Row(sequence_id=8, cal_year=2000, cal_period=8, start_date=date(2000, 7, 16), end_date=date(2000, 8, 12), days_in_period=28)
    , Row(sequence_id=9, cal_year=2000, cal_period=9, start_date=date(2000, 8, 13), end_date=date(2000, 9, 9), days_in_period=28)
    , Row(sequence_id=10, cal_year=2000, cal_period=10, start_date=date(2000, 9, 10), end_date=date(2000, 10, 7), days_in_period=28)
    , Row(sequence_id=11, cal_year=2000, cal_period=11, start_date=date(2000, 10, 8), end_date=date(2000, 11, 4), days_in_period=28)
    , Row(sequence_id=12, cal_year=2000, cal_period=12, start_date=date(2000, 11, 5), end_date=date(2000, 12, 2), days_in_period=28)
    , Row(sequence_id=13, cal_year=2000, cal_period=13, start_date=date(2000, 12, 3), end_date=date(2000, 12, 30), days_in_period=28)
    , Row(sequence_id=14, cal_year=2001, cal_period=1, start_date=date(2000, 12, 31), end_date=date(2001, 1, 27), days_in_period=28)
    , Row(sequence_id=15, cal_year=2001, cal_period=2, start_date=date(2001, 1, 28), end_date=date(2001, 2, 24), days_in_period=28)
    , Row(sequence_id=16, cal_year=2001, cal_period=3, start_date=date(2001, 2, 25), end_date=date(2001, 3, 24), days_in_period=28)
    , Row(sequence_id=17, cal_year=2001, cal_period=4, start_date=date(2001, 3, 25), end_date=date(2001, 4, 21), days_in_period=28)
    , Row(sequence_id=18, cal_year=2001, cal_period=5, start_date=date(2001, 4, 22), end_date=date(2001, 5, 19), days_in_period=28)
    , Row(sequence_id=19, cal_year=2001, cal_period=6, start_date=date(2001, 5, 20), end_date=date(2001, 6, 16), days_in_period=28)
    , Row(sequence_id=20, cal_year=2001, cal_period=7, start_date=date(2001, 6, 17), end_date=date(2001, 7, 14), days_in_period=28)
    , Row(sequence_id=21, cal_year=2001, cal_period=8, start_date=date(2001, 7, 15), end_date=date(2001, 8, 11), days_in_period=28)
    , Row(sequence_id=22, cal_year=2001, cal_period=9, start_date=date(2001, 8, 12), end_date=date(2001, 9, 8), days_in_period=28)
    , Row(sequence_id=23, cal_year=2001, cal_period=10, start_date=date(2001, 9, 9), end_date=date(2001, 10, 6), days_in_period=28)
    , Row(sequence_id=24, cal_year=2001, cal_period=11, start_date=date(2001, 10, 7), end_date=date(2001, 11, 3), days_in_period=28)
    , Row(sequence_id=25, cal_year=2001, cal_period=12, start_date=date(2001, 11, 4), end_date=date(2001, 12, 1), days_in_period=28)
    , Row(sequence_id=26, cal_year=2001, cal_period=13, start_date=date(2001, 12, 2), end_date=date(2001, 12, 29), days_in_period=28)
    , Row(sequence_id=27, cal_year=2002, cal_period=1, start_date=date(2001, 12, 30), end_date=date(2002, 1, 26), days_in_period=28)
    , Row(sequence_id=28, cal_year=2002, cal_period=2, start_date=date(2002, 1, 27), end_date=date(2002, 2, 23), days_in_period=28)
    , Row(sequence_id=29, cal_year=2002, cal_period=3, start_date=date(2002, 2, 24), end_date=date(2002, 3, 23), days_in_period=28)
    , Row(sequence_id=30, cal_year=2002, cal_period=4, start_date=date(2002, 3, 24), end_date=date(2002, 4, 20), days_in_period=28)
    , Row(sequence_id=31, cal_year=2002, cal_period=5, start_date=date(2002, 4, 21), end_date=date(2002, 5, 18), days_in_period=28)
    , Row(sequence_id=32, cal_year=2002, cal_period=6, start_date=date(2002, 5, 19), end_date=date(2002, 6, 15), days_in_period=28)
    , Row(sequence_id=33, cal_year=2002, cal_period=7, start_date=date(2002, 6, 16), end_date=date(2002, 7, 13), days_in_period=28)
    , Row(sequence_id=34, cal_year=2002, cal_period=8, start_date=date(2002, 7, 14), end_date=date(2002, 8, 10), days_in_period=28)
    , Row(sequence_id=35, cal_year=2002, cal_period=9, start_date=date(2002, 8, 11), end_date=date(2002, 9, 7), days_in_period=28)
    , Row(sequence_id=36, cal_year=2002, cal_period=10, start_date=date(2002, 9, 8), end_date=date(2002, 10, 5), days_in_period=28)
    , Row(sequence_id=37, cal_year=2002, cal_period=11, start_date=date(2002, 10, 6), end_date=date(2002, 11, 2), days_in_period=28)
    , Row(sequence_id=38, cal_year=2002, cal_period=12, start_date=date(2002, 11, 3), end_date=date(2002, 11, 30), days_in_period=28)
    , Row(sequence_id=39, cal_year=2002, cal_period=13, start_date=date(2002, 12, 1), end_date=date(2002, 12, 28), days_in_period=28)
    , Row(sequence_id=40, cal_year=2003, cal_period=1, start_date=date(2002, 12, 29), end_date=date(2003, 1, 25), days_in_period=28)
    , Row(sequence_id=41, cal_year=2003, cal_period=2, start_date=date(2003, 1, 26), end_date=date(2003, 2, 22), days_in_period=28)
    , Row(sequence_id=42, cal_year=2003, cal_period=3, start_date=date(2003, 2, 23), end_date=date(2003, 3, 22), days_in_period=28)
    , Row(sequence_id=43, cal_year=2003, cal_period=4, start_date=date(2003, 3, 23), end_date=date(2003, 4, 19), days_in_period=28)
    , Row(sequence_id=44, cal_year=2003, cal_period=5, start_date=date(2003, 4, 20), end_date=date(2003, 5, 17), days_in_period=28)
    , Row(sequence_id=45, cal_year=2003, cal_period=6, start_date=date(2003, 5, 18), end_date=date(2003, 6, 14), days_in_period=28)
    , Row(sequence_id=46, cal_year=2003, cal_period=7, start_date=date(2003, 6, 15), end_date=date(2003, 7, 12), days_in_period=28)
    , Row(sequence_id=47, cal_year=2003, cal_period=8, start_date=date(2003, 7, 13), end_date=date(2003, 8, 9), days_in_period=28)
    , Row(sequence_id=48, cal_year=2003, cal_period=9, start_date=date(2003, 8, 10), end_date=date(2003, 9, 6), days_in_period=28)
    , Row(sequence_id=49, cal_year=2003, cal_period=10, start_date=date(2003, 9, 7), end_date=date(2003, 10, 4), days_in_period=28)
    , Row(sequence_id=50, cal_year=2003, cal_period=11, start_date=date(2003, 10, 5), end_date=date(2003, 11, 1), days_in_period=28)
    , Row(sequence_id=51, cal_year=2003, cal_period=12, start_date=date(2003, 11, 2), end_date=date(2003, 11, 29), days_in_period=28)
    , Row(sequence_id=52, cal_year=2003, cal_period=13, start_date=date(2003, 11, 30), end_date=date(2004, 1, 3), days_in_period=35)
    , Row(sequence_id=53, cal_year=2004, cal_period=1, start_date=date(2004, 1, 4), end_date=date(2004, 1, 31), days_in_period=28)
    , Row(sequence_id=54, cal_year=2004, cal_period=2, start_date=date(2004, 2, 1), end_date=date(2004, 2, 28), days_in_period=28)
    , Row(sequence_id=55, cal_year=2004, cal_period=3, start_date=date(2004, 2, 29), end_date=date(2004, 3, 27), days_in_period=28)
    , Row(sequence_id=56, cal_year=2004, cal_period=4, start_date=date(2004, 3, 28), end_date=date(2004, 4, 24), days_in_period=28)
    , Row(sequence_id=57, cal_year=2004, cal_period=5, start_date=date(2004, 4, 25), end_date=date(2004, 5, 22), days_in_period=28)
    , Row(sequence_id=58, cal_year=2004, cal_period=6, start_date=date(2004, 5, 23), end_date=date(2004, 6, 19), days_in_period=28)
    , Row(sequence_id=59, cal_year=2004, cal_period=7, start_date=date(2004, 6, 20), end_date=date(2004, 7, 17), days_in_period=28)
    , Row(sequence_id=60, cal_year=2004, cal_period=8, start_date=date(2004, 7, 18), end_date=date(2004, 8, 14), days_in_period=28)
    , Row(sequence_id=61, cal_year=2004, cal_period=9, start_date=date(2004, 8, 15), end_date=date(2004, 9, 11), days_in_period=28)
    , Row(sequence_id=62, cal_year=2004, cal_period=10, start_date=date(2004, 9, 12), end_date=date(2004, 10, 9), days_in_period=28)
    , Row(sequence_id=63, cal_year=2004, cal_period=11, start_date=date(2004, 10, 10), end_date=date(2004, 11, 6), days_in_period=28)
    , Row(sequence_id=64, cal_year=2004, cal_period=12, start_date=date(2004, 11, 7), end_date=date(2004, 12, 4), days_in_period=28)
    , Row(sequence_id=65, cal_year=2004, cal_period=13, start_date=date(2004, 12, 5), end_date=date(2005, 1, 1), days_in_period=28)
    , Row(sequence_id=66, cal_year=2005, cal_period=1, start_date=date(2005, 1, 2), end_date=date(2005, 1, 29), days_in_period=28)
    , Row(sequence_id=67, cal_year=2005, cal_period=2, start_date=date(2005, 1, 30), end_date=date(2005, 2, 26), days_in_period=28)
    , Row(sequence_id=68, cal_year=2005, cal_period=3, start_date=date(2005, 2, 27), end_date=date(2005, 3, 26), days_in_period=28)
    , Row(sequence_id=69, cal_year=2005, cal_period=4, start_date=date(2005, 3, 27), end_date=date(2005, 4, 23), days_in_period=28)
    , Row(sequence_id=70, cal_year=2005, cal_period=5, start_date=date(2005, 4, 24), end_date=date(2005, 5, 21), days_in_period=28)
    , Row(sequence_id=71, cal_year=2005, cal_period=6, start_date=date(2005, 5, 22), end_date=date(2005, 6, 18), days_in_period=28)
    , Row(sequence_id=72, cal_year=2005, cal_period=7, start_date=date(2005, 6, 19), end_date=date(2005, 7, 16), days_in_period=28)
    , Row(sequence_id=73, cal_year=2005, cal_period=8, start_date=date(2005, 7, 17), end_date=date(2005, 8, 13), days_in_period=28)
    , Row(sequence_id=74, cal_year=2005, cal_period=9, start_date=date(2005, 8, 14), end_date=date(2005, 9, 10), days_in_period=28)
    , Row(sequence_id=75, cal_year=2005, cal_period=10, start_date=date(2005, 9, 11), end_date=date(2005, 10, 8), days_in_period=28)
    , Row(sequence_id=76, cal_year=2005, cal_period=11, start_date=date(2005, 10, 9), end_date=date(2005, 11, 5), days_in_period=28)
    , Row(sequence_id=77, cal_year=2005, cal_period=12, start_date=date(2005, 11, 6), end_date=date(2005, 12, 3), days_in_period=28)
    , Row(sequence_id=78, cal_year=2005, cal_period=13, start_date=date(2005, 12, 4), end_date=date(2005, 12, 31), days_in_period=28)
    , Row(sequence_id=79, cal_year=2006, cal_period=1, start_date=date(2006, 1, 1), end_date=date(2006, 1, 28), days_in_period=28)
    , Row(sequence_id=80, cal_year=2006, cal_period=2, start_date=date(2006, 1, 29), end_date=date(2006, 2, 25), days_in_period=28)
    , Row(sequence_id=81, cal_year=2006, cal_period=3, start_date=date(2006, 2, 26), end_date=date(2006, 3, 25), days_in_period=28)
    , Row(sequence_id=82, cal_year=2006, cal_period=4, start_date=date(2006, 3, 26), end_date=date(2006, 4, 22), days_in_period=28)
    , Row(sequence_id=83, cal_year=2006, cal_period=5, start_date=date(2006, 4, 23), end_date=date(2006, 5, 20), days_in_period=28)
    , Row(sequence_id=84, cal_year=2006, cal_period=6, start_date=date(2006, 5, 21), end_date=date(2006, 6, 17), days_in_period=28)
    , Row(sequence_id=85, cal_year=2006, cal_period=7, start_date=date(2006, 6, 18), end_date=date(2006, 7, 15), days_in_period=28)
    , Row(sequence_id=86, cal_year=2006, cal_period=8, start_date=date(2006, 7, 16), end_date=date(2006, 8, 12), days_in_period=28)
    , Row(sequence_id=87, cal_year=2006, cal_period=9, start_date=date(2006, 8, 13), end_date=date(2006, 9, 9), days_in_period=28)
    , Row(sequence_id=88, cal_year=2006, cal_period=10, start_date=date(2006, 9, 10), end_date=date(2006, 10, 7), days_in_period=28)
    , Row(sequence_id=89, cal_year=2006, cal_period=11, start_date=date(2006, 10, 8), end_date=date(2006, 11, 4), days_in_period=28)
    , Row(sequence_id=90, cal_year=2006, cal_period=12, start_date=date(2006, 11, 5), end_date=date(2006, 12, 2), days_in_period=28)
    , Row(sequence_id=91, cal_year=2006, cal_period=13, start_date=date(2006, 12, 3), end_date=date(2006, 12, 30), days_in_period=28)
    , Row(sequence_id=92, cal_year=2007, cal_period=1, start_date=date(2006, 12, 31), end_date=date(2007, 1, 27), days_in_period=28)
    , Row(sequence_id=93, cal_year=2007, cal_period=2, start_date=date(2007, 1, 28), end_date=date(2007, 2, 24), days_in_period=28)
    , Row(sequence_id=94, cal_year=2007, cal_period=3, start_date=date(2007, 2, 25), end_date=date(2007, 3, 24), days_in_period=28)
    , Row(sequence_id=95, cal_year=2007, cal_period=4, start_date=date(2007, 3, 25), end_date=date(2007, 4, 21), days_in_period=28)
    , Row(sequence_id=96, cal_year=2007, cal_period=5, start_date=date(2007, 4, 22), end_date=date(2007, 5, 19), days_in_period=28)
    , Row(sequence_id=97, cal_year=2007, cal_period=6, start_date=date(2007, 5, 20), end_date=date(2007, 6, 16), days_in_period=28)
    , Row(sequence_id=98, cal_year=2007, cal_period=7, start_date=date(2007, 6, 17), end_date=date(2007, 7, 14), days_in_period=28)
    , Row(sequence_id=99, cal_year=2007, cal_period=8, start_date=date(2007, 7, 15), end_date=date(2007, 8, 11), days_in_period=28)
    , Row(sequence_id=100, cal_year=2007, cal_period=9, start_date=date(2007, 8, 12), end_date=date(2007, 9, 8), days_in_period=28)
    , Row(sequence_id=101, cal_year=2007, cal_period=10, start_date=date(2007, 9, 9), end_date=date(2007, 10, 6), days_in_period=28)
    , Row(sequence_id=102, cal_year=2007, cal_period=11, start_date=date(2007, 10, 7), end_date=date(2007, 11, 3), days_in_period=28)
    , Row(sequence_id=103, cal_year=2007, cal_period=12, start_date=date(2007, 11, 4), end_date=date(2007, 12, 1), days_in_period=28)
    , Row(sequence_id=104, cal_year=2007, cal_period=13, start_date=date(2007, 12, 2), end_date=date(2007, 12, 29), days_in_period=28)
    , Row(sequence_id=105, cal_year=2008, cal_period=1, start_date=date(2007, 12, 30), end_date=date(2008, 1, 26), days_in_period=28)
    , Row(sequence_id=106, cal_year=2008, cal_period=2, start_date=date(2008, 1, 27), end_date=date(2008, 2, 23), days_in_period=28)
    , Row(sequence_id=107, cal_year=2008, cal_period=3, start_date=date(2008, 2, 24), end_date=date(2008, 3, 22), days_in_period=28)
    , Row(sequence_id=108, cal_year=2008, cal_period=4, start_date=date(2008, 3, 23), end_date=date(2008, 4, 19), days_in_period=28)
    , Row(sequence_id=109, cal_year=2008, cal_period=5, start_date=date(2008, 4, 20), end_date=date(2008, 5, 17), days_in_period=28)
    , Row(sequence_id=110, cal_year=2008, cal_period=6, start_date=date(2008, 5, 18), end_date=date(2008, 6, 14), days_in_period=28)
    , Row(sequence_id=111, cal_year=2008, cal_period=7, start_date=date(2008, 6, 15), end_date=date(2008, 7, 12), days_in_period=28)
    , Row(sequence_id=112, cal_year=2008, cal_period=8, start_date=date(2008, 7, 13), end_date=date(2008, 8, 9), days_in_period=28)
    , Row(sequence_id=113, cal_year=2008, cal_period=9, start_date=date(2008, 8, 10), end_date=date(2008, 9, 6), days_in_period=28)
    , Row(sequence_id=114, cal_year=2008, cal_period=10, start_date=date(2008, 9, 7), end_date=date(2008, 10, 4), days_in_period=28)
    , Row(sequence_id=115, cal_year=2008, cal_period=11, start_date=date(2008, 10, 5), end_date=date(2008, 11, 1), days_in_period=28)
    , Row(sequence_id=116, cal_year=2008, cal_period=12, start_date=date(2008, 11, 2), end_date=date(2008, 11, 29), days_in_period=28)
    , Row(sequence_id=117, cal_year=2008, cal_period=13, start_date=date(2008, 11, 30), end_date=date(2009, 1, 3), days_in_period=35)
    , Row(sequence_id=118, cal_year=2009, cal_period=1, start_date=date(2009, 1, 4), end_date=date(2009, 1, 31), days_in_period=28)
    , Row(sequence_id=119, cal_year=2009, cal_period=2, start_date=date(2009, 2, 1), end_date=date(2009, 2, 28), days_in_period=28)
    , Row(sequence_id=120, cal_year=2009, cal_period=3, start_date=date(2009, 3, 1), end_date=date(2009, 3, 28), days_in_period=28)
    , Row(sequence_id=121, cal_year=2009, cal_period=4, start_date=date(2009, 3, 29), end_date=date(2009, 4, 25), days_in_period=28)
    , Row(sequence_id=122, cal_year=2009, cal_period=5, start_date=date(2009, 4, 26), end_date=date(2009, 5, 23), days_in_period=28)
    , Row(sequence_id=123, cal_year=2009, cal_period=6, start_date=date(2009, 5, 24), end_date=date(2009, 6, 20), days_in_period=28)
    , Row(sequence_id=124, cal_year=2009, cal_period=7, start_date=date(2009, 6, 21), end_date=date(2009, 7, 18), days_in_period=28)
    , Row(sequence_id=125, cal_year=2009, cal_period=8, start_date=date(2009, 7, 19), end_date=date(2009, 8, 15), days_in_period=28)
    , Row(sequence_id=126, cal_year=2009, cal_period=9, start_date=date(2009, 8, 16), end_date=date(2009, 9, 12), days_in_period=28)
    , Row(sequence_id=127, cal_year=2009, cal_period=10, start_date=date(2009, 9, 13), end_date=date(2009, 10, 10), days_in_period=28)
    , Row(sequence_id=128, cal_year=2009, cal_period=11, start_date=date(2009, 10, 11), end_date=date(2009, 11, 7), days_in_period=28)
    , Row(sequence_id=129, cal_year=2009, cal_period=12, start_date=date(2009, 11, 8), end_date=date(2009, 12, 5), days_in_period=28)
    , Row(sequence_id=130, cal_year=2009, cal_period=13, start_date=date(2009, 12, 6), end_date=date(2010, 1, 2), days_in_period=28)
    , Row(sequence_id=131, cal_year=2010, cal_period=1, start_date=date(2010, 1, 3), end_date=date(2010, 1, 30), days_in_period=28)
    , Row(sequence_id=132, cal_year=2010, cal_period=2, start_date=date(2010, 1, 31), end_date=date(2010, 2, 27), days_in_period=28)
    , Row(sequence_id=133, cal_year=2010, cal_period=3, start_date=date(2010, 2, 28), end_date=date(2010, 3, 27), days_in_period=28)
    , Row(sequence_id=134, cal_year=2010, cal_period=4, start_date=date(2010, 3, 28), end_date=date(2010, 4, 24), days_in_period=28)
    , Row(sequence_id=135, cal_year=2010, cal_period=5, start_date=date(2010, 4, 25), end_date=date(2010, 5, 22), days_in_period=28)
    , Row(sequence_id=136, cal_year=2010, cal_period=6, start_date=date(2010, 5, 23), end_date=date(2010, 6, 19), days_in_period=28)
    , Row(sequence_id=137, cal_year=2010, cal_period=7, start_date=date(2010, 6, 20), end_date=date(2010, 7, 17), days_in_period=28)
    , Row(sequence_id=138, cal_year=2010, cal_period=8, start_date=date(2010, 7, 18), end_date=date(2010, 8, 14), days_in_period=28)
    , Row(sequence_id=139, cal_year=2010, cal_period=9, start_date=date(2010, 8, 15), end_date=date(2010, 9, 11), days_in_period=28)
    , Row(sequence_id=140, cal_year=2010, cal_period=10, start_date=date(2010, 9, 12), end_date=date(2010, 10, 9), days_in_period=28)
    , Row(sequence_id=141, cal_year=2010, cal_period=11, start_date=date(2010, 10, 10), end_date=date(2010, 11, 6), days_in_period=28)
    , Row(sequence_id=142, cal_year=2010, cal_period=12, start_date=date(2010, 11, 7), end_date=date(2010, 12, 4), days_in_period=28)
    , Row(sequence_id=143, cal_year=2010, cal_period=13, start_date=date(2010, 12, 5), end_date=date(2011, 1, 1), days_in_period=28)
    , Row(sequence_id=144, cal_year=2011, cal_period=1, start_date=date(2011, 1, 2), end_date=date(2011, 1, 29), days_in_period=28)
    , Row(sequence_id=145, cal_year=2011, cal_period=2, start_date=date(2011, 1, 30), end_date=date(2011, 2, 26), days_in_period=28)
    , Row(sequence_id=146, cal_year=2011, cal_period=3, start_date=date(2011, 2, 27), end_date=date(2011, 3, 26), days_in_period=28)
    , Row(sequence_id=147, cal_year=2011, cal_period=4, start_date=date(2011, 3, 27), end_date=date(2011, 4, 23), days_in_period=28)
    , Row(sequence_id=148, cal_year=2011, cal_period=5, start_date=date(2011, 4, 24), end_date=date(2011, 5, 21), days_in_period=28)
    , Row(sequence_id=149, cal_year=2011, cal_period=6, start_date=date(2011, 5, 22), end_date=date(2011, 6, 18), days_in_period=28)
    , Row(sequence_id=150, cal_year=2011, cal_period=7, start_date=date(2011, 6, 19), end_date=date(2011, 7, 16), days_in_period=28)
    , Row(sequence_id=151, cal_year=2011, cal_period=8, start_date=date(2011, 7, 17), end_date=date(2011, 8, 13), days_in_period=28)
    , Row(sequence_id=152, cal_year=2011, cal_period=9, start_date=date(2011, 8, 14), end_date=date(2011, 9, 10), days_in_period=28)
    , Row(sequence_id=153, cal_year=2011, cal_period=10, start_date=date(2011, 9, 11), end_date=date(2011, 10, 8), days_in_period=28)
    , Row(sequence_id=154, cal_year=2011, cal_period=11, start_date=date(2011, 10, 9), end_date=date(2011, 11, 5), days_in_period=28)
    , Row(sequence_id=155, cal_year=2011, cal_period=12, start_date=date(2011, 11, 6), end_date=date(2011, 12, 3), days_in_period=28)
    , Row(sequence_id=156, cal_year=2011, cal_period=13, start_date=date(2011, 12, 4), end_date=date(2011, 12, 31), days_in_period=28)
    , Row(sequence_id=157, cal_year=2012, cal_period=1, start_date=date(2012, 1, 1), end_date=date(2012, 1, 28), days_in_period=28)
    , Row(sequence_id=158, cal_year=2012, cal_period=2, start_date=date(2012, 1, 29), end_date=date(2012, 2, 25), days_in_period=28)
    , Row(sequence_id=159, cal_year=2012, cal_period=3, start_date=date(2012, 2, 26), end_date=date(2012, 3, 24), days_in_period=28)
    , Row(sequence_id=160, cal_year=2012, cal_period=4, start_date=date(2012, 3, 25), end_date=date(2012, 4, 21), days_in_period=28)
    , Row(sequence_id=161, cal_year=2012, cal_period=5, start_date=date(2012, 4, 22), end_date=date(2012, 5, 19), days_in_period=28)
    , Row(sequence_id=162, cal_year=2012, cal_period=6, start_date=date(2012, 5, 20), end_date=date(2012, 6, 16), days_in_period=28)
    , Row(sequence_id=163, cal_year=2012, cal_period=7, start_date=date(2012, 6, 17), end_date=date(2012, 7, 14), days_in_period=28)
    , Row(sequence_id=164, cal_year=2012, cal_period=8, start_date=date(2012, 7, 15), end_date=date(2012, 8, 11), days_in_period=28)
    , Row(sequence_id=165, cal_year=2012, cal_period=9, start_date=date(2012, 8, 12), end_date=date(2012, 9, 8), days_in_period=28)
    , Row(sequence_id=166, cal_year=2012, cal_period=10, start_date=date(2012, 9, 9), end_date=date(2012, 10, 6), days_in_period=28)
    , Row(sequence_id=167, cal_year=2012, cal_period=11, start_date=date(2012, 10, 7), end_date=date(2012, 11, 3), days_in_period=28)
    , Row(sequence_id=168, cal_year=2012, cal_period=12, start_date=date(2012, 11, 4), end_date=date(2012, 12, 1), days_in_period=28)
    , Row(sequence_id=169, cal_year=2012, cal_period=13, start_date=date(2012, 12, 2), end_date=date(2012, 12, 29), days_in_period=28)
    , Row(sequence_id=170, cal_year=2013, cal_period=1, start_date=date(2012, 12, 30), end_date=date(2013, 1, 26), days_in_period=28)
    , Row(sequence_id=171, cal_year=2013, cal_period=2, start_date=date(2013, 1, 27), end_date=date(2013, 2, 23), days_in_period=28)
    , Row(sequence_id=172, cal_year=2013, cal_period=3, start_date=date(2013, 2, 24), end_date=date(2013, 3, 23), days_in_period=28)
    , Row(sequence_id=173, cal_year=2013, cal_period=4, start_date=date(2013, 3, 24), end_date=date(2013, 4, 20), days_in_period=28)
    , Row(sequence_id=174, cal_year=2013, cal_period=5, start_date=date(2013, 4, 21), end_date=date(2013, 5, 18), days_in_period=28)
    , Row(sequence_id=175, cal_year=2013, cal_period=6, start_date=date(2013, 5, 19), end_date=date(2013, 6, 15), days_in_period=28)
    , Row(sequence_id=176, cal_year=2013, cal_period=7, start_date=date(2013, 6, 16), end_date=date(2013, 7, 13), days_in_period=28)
    , Row(sequence_id=177, cal_year=2013, cal_period=8, start_date=date(2013, 7, 14), end_date=date(2013, 8, 10), days_in_period=28)
    , Row(sequence_id=178, cal_year=2013, cal_period=9, start_date=date(2013, 8, 11), end_date=date(2013, 9, 7), days_in_period=28)
    , Row(sequence_id=179, cal_year=2013, cal_period=10, start_date=date(2013, 9, 8), end_date=date(2013, 10, 5), days_in_period=28)
    , Row(sequence_id=180, cal_year=2013, cal_period=11, start_date=date(2013, 10, 6), end_date=date(2013, 11, 2), days_in_period=28)
    , Row(sequence_id=181, cal_year=2013, cal_period=12, start_date=date(2013, 11, 3), end_date=date(2013, 11, 30), days_in_period=28)
    , Row(sequence_id=182, cal_year=2013, cal_period=13, start_date=date(2013, 12, 1), end_date=date(2013, 12, 28), days_in_period=28)
    , Row(sequence_id=183, cal_year=2014, cal_period=1, start_date=date(2013, 12, 29), end_date=date(2014, 1, 25), days_in_period=28)
    , Row(sequence_id=184, cal_year=2014, cal_period=2, start_date=date(2014, 1, 26), end_date=date(2014, 2, 22), days_in_period=28)
    , Row(sequence_id=185, cal_year=2014, cal_period=3, start_date=date(2014, 2, 23), end_date=date(2014, 3, 22), days_in_period=28)
    , Row(sequence_id=186, cal_year=2014, cal_period=4, start_date=date(2014, 3, 23), end_date=date(2014, 4, 19), days_in_period=28)
    , Row(sequence_id=187, cal_year=2014, cal_period=5, start_date=date(2014, 4, 20), end_date=date(2014, 5, 17), days_in_period=28)
    , Row(sequence_id=188, cal_year=2014, cal_period=6, start_date=date(2014, 5, 18), end_date=date(2014, 6, 14), days_in_period=28)
    , Row(sequence_id=189, cal_year=2014, cal_period=7, start_date=date(2014, 6, 15), end_date=date(2014, 7, 12), days_in_period=28)
    , Row(sequence_id=190, cal_year=2014, cal_period=8, start_date=date(2014, 7, 13), end_date=date(2014, 8, 9), days_in_period=28)
    , Row(sequence_id=191, cal_year=2014, cal_period=9, start_date=date(2014, 8, 10), end_date=date(2014, 9, 6), days_in_period=28)
    , Row(sequence_id=192, cal_year=2014, cal_period=10, start_date=date(2014, 9, 7), end_date=date(2014, 10, 4), days_in_period=28)
    , Row(sequence_id=193, cal_year=2014, cal_period=11, start_date=date(2014, 10, 5), end_date=date(2014, 11, 1), days_in_period=28)
    , Row(sequence_id=194, cal_year=2014, cal_period=12, start_date=date(2014, 11, 2), end_date=date(2014, 11, 29), days_in_period=28)
    , Row(sequence_id=195, cal_year=2014, cal_period=13, start_date=date(2014, 11, 30), end_date=date(2015, 1, 3), days_in_period=35)
    , Row(sequence_id=196, cal_year=2015, cal_period=1, start_date=date(2015, 1, 4), end_date=date(2015, 1, 31), days_in_period=28)
    , Row(sequence_id=197, cal_year=2015, cal_period=2, start_date=date(2015, 2, 1), end_date=date(2015, 2, 28), days_in_period=28)
    , Row(sequence_id=198, cal_year=2015, cal_period=3, start_date=date(2015, 3, 1), end_date=date(2015, 3, 28), days_in_period=28)
    , Row(sequence_id=199, cal_year=2015, cal_period=4, start_date=date(2015, 3, 29), end_date=date(2015, 4, 25), days_in_period=28)
    , Row(sequence_id=200, cal_year=2015, cal_period=5, start_date=date(2015, 4, 26), end_date=date(2015, 5, 23), days_in_period=28)
    , Row(sequence_id=201, cal_year=2015, cal_period=6, start_date=date(2015, 5, 24), end_date=date(2015, 6, 20), days_in_period=28)
    , Row(sequence_id=202, cal_year=2015, cal_period=7, start_date=date(2015, 6, 21), end_date=date(2015, 7, 18), days_in_period=28)
    , Row(sequence_id=203, cal_year=2015, cal_period=8, start_date=date(2015, 7, 19), end_date=date(2015, 8, 15), days_in_period=28)
    , Row(sequence_id=204, cal_year=2015, cal_period=9, start_date=date(2015, 8, 16), end_date=date(2015, 9, 12), days_in_period=28)
    , Row(sequence_id=205, cal_year=2015, cal_period=10, start_date=date(2015, 9, 13), end_date=date(2015, 10, 10), days_in_period=28)
    , Row(sequence_id=206, cal_year=2015, cal_period=11, start_date=date(2015, 10, 11), end_date=date(2015, 11, 7), days_in_period=28)
    , Row(sequence_id=207, cal_year=2015, cal_period=12, start_date=date(2015, 11, 8), end_date=date(2015, 12, 5), days_in_period=28)
    , Row(sequence_id=208, cal_year=2015, cal_period=13, start_date=date(2015, 12, 6), end_date=date(2016, 1, 2), days_in_period=28)
    , Row(sequence_id=209, cal_year=2016, cal_period=1, start_date=date(2016, 1, 3), end_date=date(2016, 1, 30), days_in_period=28)
    , Row(sequence_id=210, cal_year=2016, cal_period=2, start_date=date(2016, 1, 31), end_date=date(2016, 2, 27), days_in_period=28)
    , Row(sequence_id=211, cal_year=2016, cal_period=3, start_date=date(2016, 2, 28), end_date=date(2016, 3, 26), days_in_period=28)
    , Row(sequence_id=212, cal_year=2016, cal_period=4, start_date=date(2016, 3, 27), end_date=date(2016, 4, 23), days_in_period=28)
    , Row(sequence_id=213, cal_year=2016, cal_period=5, start_date=date(2016, 4, 24), end_date=date(2016, 5, 21), days_in_period=28)
    , Row(sequence_id=214, cal_year=2016, cal_period=6, start_date=date(2016, 5, 22), end_date=date(2016, 6, 18), days_in_period=28)
    , Row(sequence_id=215, cal_year=2016, cal_period=7, start_date=date(2016, 6, 19), end_date=date(2016, 7, 16), days_in_period=28)
    , Row(sequence_id=216, cal_year=2016, cal_period=8, start_date=date(2016, 7, 17), end_date=date(2016, 8, 13), days_in_period=28)
    , Row(sequence_id=217, cal_year=2016, cal_period=9, start_date=date(2016, 8, 14), end_date=date(2016, 9, 10), days_in_period=28)
    , Row(sequence_id=218, cal_year=2016, cal_period=10, start_date=date(2016, 9, 11), end_date=date(2016, 10, 8), days_in_period=28)
    , Row(sequence_id=219, cal_year=2016, cal_period=11, start_date=date(2016, 10, 9), end_date=date(2016, 11, 5), days_in_period=28)
    , Row(sequence_id=220, cal_year=2016, cal_period=12, start_date=date(2016, 11, 6), end_date=date(2016, 12, 3), days_in_period=28)
    , Row(sequence_id=221, cal_year=2016, cal_period=13, start_date=date(2016, 12, 4), end_date=date(2016, 12, 31), days_in_period=28)
    , Row(sequence_id=222, cal_year=2017, cal_period=1, start_date=date(2017, 1, 1), end_date=date(2017, 1, 28), days_in_period=28)
    , Row(sequence_id=223, cal_year=2017, cal_period=2, start_date=date(2017, 1, 29), end_date=date(2017, 2, 25), days_in_period=28)
    , Row(sequence_id=224, cal_year=2017, cal_period=3, start_date=date(2017, 2, 26), end_date=date(2017, 3, 25), days_in_period=28)
    , Row(sequence_id=225, cal_year=2017, cal_period=4, start_date=date(2017, 3, 26), end_date=date(2017, 4, 22), days_in_period=28)
    , Row(sequence_id=226, cal_year=2017, cal_period=5, start_date=date(2017, 4, 23), end_date=date(2017, 5, 20), days_in_period=28)
    , Row(sequence_id=227, cal_year=2017, cal_period=6, start_date=date(2017, 5, 21), end_date=date(2017, 6, 17), days_in_period=28)
    , Row(sequence_id=228, cal_year=2017, cal_period=7, start_date=date(2017, 6, 18), end_date=date(2017, 7, 15), days_in_period=28)
    , Row(sequence_id=229, cal_year=2017, cal_period=8, start_date=date(2017, 7, 16), end_date=date(2017, 8, 12), days_in_period=28)
    , Row(sequence_id=230, cal_year=2017, cal_period=9, start_date=date(2017, 8, 13), end_date=date(2017, 9, 9), days_in_period=28)
    , Row(sequence_id=231, cal_year=2017, cal_period=10, start_date=date(2017, 9, 10), end_date=date(2017, 10, 7), days_in_period=28)
    , Row(sequence_id=232, cal_year=2017, cal_period=11, start_date=date(2017, 10, 8), end_date=date(2017, 11, 4), days_in_period=28)
    , Row(sequence_id=233, cal_year=2017, cal_period=12, start_date=date(2017, 11, 5), end_date=date(2017, 12, 2), days_in_period=28)
    , Row(sequence_id=234, cal_year=2017, cal_period=13, start_date=date(2017, 12, 3), end_date=date(2017, 12, 30), days_in_period=28)
    , Row(sequence_id=235, cal_year=2018, cal_period=1, start_date=date(2017, 12, 31), end_date=date(2018, 1, 27), days_in_period=28)
    , Row(sequence_id=236, cal_year=2018, cal_period=2, start_date=date(2018, 1, 28), end_date=date(2018, 2, 24), days_in_period=28)
    , Row(sequence_id=237, cal_year=2018, cal_period=3, start_date=date(2018, 2, 25), end_date=date(2018, 3, 24), days_in_period=28)
    , Row(sequence_id=238, cal_year=2018, cal_period=4, start_date=date(2018, 3, 25), end_date=date(2018, 4, 21), days_in_period=28)
    , Row(sequence_id=239, cal_year=2018, cal_period=5, start_date=date(2018, 4, 22), end_date=date(2018, 5, 19), days_in_period=28)
    , Row(sequence_id=240, cal_year=2018, cal_period=6, start_date=date(2018, 5, 20), end_date=date(2018, 6, 16), days_in_period=28)
    , Row(sequence_id=241, cal_year=2018, cal_period=7, start_date=date(2018, 6, 17), end_date=date(2018, 7, 14), days_in_period=28)
    , Row(sequence_id=242, cal_year=2018, cal_period=8, start_date=date(2018, 7, 15), end_date=date(2018, 8, 11), days_in_period=28)
    , Row(sequence_id=243, cal_year=2018, cal_period=9, start_date=date(2018, 8, 12), end_date=date(2018, 9, 8), days_in_period=28)
    , Row(sequence_id=244, cal_year=2018, cal_period=10, start_date=date(2018, 9, 9), end_date=date(2018, 10, 6), days_in_period=28)
    , Row(sequence_id=245, cal_year=2018, cal_period=11, start_date=date(2018, 10, 7), end_date=date(2018, 11, 3), days_in_period=28)
    , Row(sequence_id=246, cal_year=2018, cal_period=12, start_date=date(2018, 11, 4), end_date=date(2018, 12, 1), days_in_period=28)
    , Row(sequence_id=247, cal_year=2018, cal_period=13, start_date=date(2018, 12, 2), end_date=date(2018, 12, 29), days_in_period=28)
    , Row(sequence_id=248, cal_year=2019, cal_period=1, start_date=date(2018, 12, 30), end_date=date(2019, 1, 26), days_in_period=28)
    , Row(sequence_id=249, cal_year=2019, cal_period=2, start_date=date(2019, 1, 27), end_date=date(2019, 2, 23), days_in_period=28)
    , Row(sequence_id=250, cal_year=2019, cal_period=3, start_date=date(2019, 2, 24), end_date=date(2019, 3, 23), days_in_period=28)
    , Row(sequence_id=251, cal_year=2019, cal_period=4, start_date=date(2019, 3, 24), end_date=date(2019, 4, 20), days_in_period=28)
    , Row(sequence_id=252, cal_year=2019, cal_period=5, start_date=date(2019, 4, 21), end_date=date(2019, 5, 18), days_in_period=28)
    , Row(sequence_id=253, cal_year=2019, cal_period=6, start_date=date(2019, 5, 19), end_date=date(2019, 6, 15), days_in_period=28)
    , Row(sequence_id=254, cal_year=2019, cal_period=7, start_date=date(2019, 6, 16), end_date=date(2019, 7, 13), days_in_period=28)
    , Row(sequence_id=255, cal_year=2019, cal_period=8, start_date=date(2019, 7, 14), end_date=date(2019, 8, 10), days_in_period=28)
    , Row(sequence_id=256, cal_year=2019, cal_period=9, start_date=date(2019, 8, 11), end_date=date(2019, 9, 7), days_in_period=28)
    , Row(sequence_id=257, cal_year=2019, cal_period=10, start_date=date(2019, 9, 8), end_date=date(2019, 10, 5), days_in_period=28)
    , Row(sequence_id=258, cal_year=2019, cal_period=11, start_date=date(2019, 10, 6), end_date=date(2019, 11, 2), days_in_period=28)
    , Row(sequence_id=259, cal_year=2019, cal_period=12, start_date=date(2019, 11, 3), end_date=date(2019, 11, 30), days_in_period=28)
    , Row(sequence_id=260, cal_year=2019, cal_period=13, start_date=date(2019, 12, 1), end_date=date(2019, 12, 28), days_in_period=28)
    , Row(sequence_id=261, cal_year=2020, cal_period=1, start_date=date(2019, 12, 29), end_date=date(2020, 1, 25), days_in_period=28)
    , Row(sequence_id=262, cal_year=2020, cal_period=2, start_date=date(2020, 1, 26), end_date=date(2020, 2, 22), days_in_period=28)
    , Row(sequence_id=263, cal_year=2020, cal_period=3, start_date=date(2020, 2, 23), end_date=date(2020, 3, 21), days_in_period=28)
    , Row(sequence_id=264, cal_year=2020, cal_period=4, start_date=date(2020, 3, 22), end_date=date(2020, 4, 18), days_in_period=28)
    , Row(sequence_id=265, cal_year=2020, cal_period=5, start_date=date(2020, 4, 19), end_date=date(2020, 5, 16), days_in_period=28)
    , Row(sequence_id=266, cal_year=2020, cal_period=6, start_date=date(2020, 5, 17), end_date=date(2020, 6, 13), days_in_period=28)
    , Row(sequence_id=267, cal_year=2020, cal_period=7, start_date=date(2020, 6, 14), end_date=date(2020, 7, 11), days_in_period=28)
    , Row(sequence_id=268, cal_year=2020, cal_period=8, start_date=date(2020, 7, 12), end_date=date(2020, 8, 8), days_in_period=28)
    , Row(sequence_id=269, cal_year=2020, cal_period=9, start_date=date(2020, 8, 9), end_date=date(2020, 9, 5), days_in_period=28)
    , Row(sequence_id=270, cal_year=2020, cal_period=10, start_date=date(2020, 9, 6), end_date=date(2020, 10, 3), days_in_period=28)
    , Row(sequence_id=271, cal_year=2020, cal_period=11, start_date=date(2020, 10, 4), end_date=date(2020, 10, 31), days_in_period=28)
    , Row(sequence_id=272, cal_year=2020, cal_period=12, start_date=date(2020, 11, 1), end_date=date(2020, 11, 28), days_in_period=28)
    , Row(sequence_id=273, cal_year=2020, cal_period=13, start_date=date(2020, 11, 29), end_date=date(2021, 1, 2), days_in_period=35)
    , Row(sequence_id=274, cal_year=2021, cal_period=1, start_date=date(2021, 1, 3), end_date=date(2021, 1, 30), days_in_period=28)
    , Row(sequence_id=275, cal_year=2021, cal_period=2, start_date=date(2021, 1, 31), end_date=date(2021, 2, 27), days_in_period=28)
    , Row(sequence_id=276, cal_year=2021, cal_period=3, start_date=date(2021, 2, 28), end_date=date(2021, 3, 27), days_in_period=28)
    , Row(sequence_id=277, cal_year=2021, cal_period=4, start_date=date(2021, 3, 28), end_date=date(2021, 4, 24), days_in_period=28)
    , Row(sequence_id=278, cal_year=2021, cal_period=5, start_date=date(2021, 4, 25), end_date=date(2021, 5, 22), days_in_period=28)
    , Row(sequence_id=279, cal_year=2021, cal_period=6, start_date=date(2021, 5, 23), end_date=date(2021, 6, 19), days_in_period=28)
    , Row(sequence_id=280, cal_year=2021, cal_period=7, start_date=date(2021, 6, 20), end_date=date(2021, 7, 17), days_in_period=28)
    , Row(sequence_id=281, cal_year=2021, cal_period=8, start_date=date(2021, 7, 18), end_date=date(2021, 8, 14), days_in_period=28)
    , Row(sequence_id=282, cal_year=2021, cal_period=9, start_date=date(2021, 8, 15), end_date=date(2021, 9, 11), days_in_period=28)
    , Row(sequence_id=283, cal_year=2021, cal_period=10, start_date=date(2021, 9, 12), end_date=date(2021, 10, 9), days_in_period=28)
    , Row(sequence_id=284, cal_year=2021, cal_period=11, start_date=date(2021, 10, 10), end_date=date(2021, 11, 6), days_in_period=28)
    , Row(sequence_id=285, cal_year=2021, cal_period=12, start_date=date(2021, 11, 7), end_date=date(2021, 12, 4), days_in_period=28)
    , Row(sequence_id=286, cal_year=2021, cal_period=13, start_date=date(2021, 12, 5), end_date=date(2022, 1, 1), days_in_period=28)

]

pc_df = None

def init_pc_df(sqlContext):

    global pc_df

    if not pc_df:
        pc_df = sqlContext.createDataFrame(pc_data)
        pc_df.cache()
    else:
        pass

    return pc_df

# API to get the period calendar data frame
def get_pc_df():

    global pc_df

    return pc_df

# Get the financial period info for a given date
def fp_info(cdate):
    return  get_pc_df().filter((cdate >= pc_df.start_date) & (cdate <= pc_df.end_date)).first()

# Get the start and end period info, nPeriods prior, relative to cdate
# First previous period is the full perior prior to current period
def fps_start_end_info(cdate, nPeriods):
    current_period_info = fp_info(cdate)
    # If cdate is the last day of the period, include that and go back nPeriods -1
    # If not, go back nPeriods.
    start_period_sequence_id = current_period_info.sequence_id - nPeriods + 1
    end_period_sequence_id = current_period_info.sequence_id;
    end_period_info = None
    if (current_period_info.end_date != cdate):
        start_period_sequence_id -= 1
        end_period_sequence_id -= 1
        #end_period_info = current_period_info
    else:
        #end_period_sequence_id -= 1
        pass
    start_period_info = get_pc_df().filter(pc_df.sequence_id == start_period_sequence_id).first()

    if not end_period_info:
        end_period_info = get_pc_df().filter(pc_df.sequence_id == end_period_sequence_id).first()

    x=start_period_info.start_date
    #print 'hello'
    #print x

    #y = Row(start_period_info= start_period_info, end_period_info= end_period_info)
    #print y
    return  Row(start_period_info= start_period_info, end_period_info= end_period_info)



def get_previous_year_start_date(ref_date, years=1):
    last_year_date = datetime.date(ref_date.year, ref_date.month, ref_date.day) + timedelta(days=-365)
    return last_year_date + datetime.timedelta(days=1)

   

def get_lkup_counts_store_sku(max_date):
    
    # cy 13 period date range
    cy_fp_end_date = max_date.first()[0]
    cy_fp_start_date = get_previous_year_start_date(cy_fp_end_date)
    # # py 13 period date range
    py_fp_end_date = cy_fp_start_date + datetime.timedelta(days=-1)
    py_fp_start_date = get_previous_year_start_date(py_fp_end_date)
   

    consolidate_apex_lookups = sqlContext.sql("select store_number, sku_number,con_lookup_key,lookup_date as lkp_dt from dpdm_prod.true_demand_lookups_master_subset")
    lkup_counts_store_sku = consolidate_apex_lookups.select('store_number', 'sku_number', 'lkp_dt',
                                                            'con_lookup_key'). \
        groupby('store_number', 'sku_number'). \
        agg(countDistinct(when(((consolidate_apex_lookups.lkp_dt >= cy_fp_start_date)
                                & (consolidate_apex_lookups.lkp_dt <= cy_fp_end_date)),
                               col('con_lookup_key')).otherwise(None)).alias("cy_lookup_count") \
            , countDistinct(when(((consolidate_apex_lookups.lkp_dt >= py_fp_start_date)
                                  & (consolidate_apex_lookups.lkp_dt <= py_fp_end_date)),
                                 col('con_lookup_key')).otherwise(None)).alias("py_lookup_count"))
    return lkup_counts_store_sku




def get_epc_lookup_cy_py(as_of_date):
    pc = sqlContext.sql("select sequence_id, fiscal_year as cal_year, fiscal_period as cal_period, start_date, end_date  from m_sci_raw_prod.aap_period_calendar")
    period_id_26 = fps_start_end_info(as_of_date, 26).start_period_info.sequence_id  # py_end
    period_id_14 = fps_start_end_info(as_of_date, 14).start_period_info.sequence_id  # py_start
    period_id_13 = fps_start_end_info(as_of_date, 13).start_period_info.sequence_id  # cy_end
    period_id_1 = fps_start_end_info(as_of_date, 1).start_period_info.sequence_id  # cy_start
    pc.createOrReplaceTempView('a')
    epc_lookup_df = sqlContext.sql("select * from dds_prod.epc_lookup_store_sku_totals")
    epc_lookup_df.registerTempTable('epc')
    query_cy_py = """select store_number,sku_number\
        		,sum(case WHEN a.sequence_id BETWEEN %d AND %d THEN lookup_count else 0 end) as lookup_count_cy\
        		,sum(case WHEN a.sequence_id BETWEEN %d AND %d THEN weighted_lookup_count else 0 end) as weighted_lookup_count_cy\
        		,sum(case WHEN a.sequence_id BETWEEN %d AND %d THEN lookup_count else 0 end) as lookup_count_py\
        		,sum(case WHEN a.sequence_id BETWEEN %d AND %d THEN weighted_lookup_count else 0 end) as weighted_lookup_count_py\
        		from epc inner join a on a.cal_year = cast(epc.fiscal_year as bigint) and\
        		a.cal_period = cast(epc.fiscal_period as bigint) where a.sequence_id between %d and %d\
        		group by store_number,sku_number""" % (period_id_13, period_id_1, period_id_13, period_id_1,
                                                       period_id_26, period_id_14, period_id_26, period_id_14,
                                                       period_id_26, period_id_1)
    epc_lookup_cy_py_df = sqlContext.sql(query_cy_py)
    return epc_lookup_cy_py_df


init_pc_df(sqlContext)
# max_date= sqlContext.sql("select max(LOOKUP_DATE) from dpdm_prod.true_demand_lookups_master_subset")
# max_date.cache()
# max_date.createOrReplaceTempView("max_date")
as_of_date = datetime.date.today()  # datetime.date(2017, 2, 26)
apex_lookups = get_lkup_counts_store_sku(max_date)
epc_lookups = get_epc_lookup_cy_py(as_of_date)

integrated_df = epc_lookups.join(apex_lookups, (apex_lookups.store_number == epc_lookups.store_number) &
                                   (apex_lookups.sku_number == epc_lookups.sku_number), "leftanti")
                                   
print(integrated_df.count())
print("EPC- count",epc_lookups.count())
print("APEX - count",apex_lookups.count())   
z.show(integrated_df)