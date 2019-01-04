import sys
import calendar
from datetime import date
import pandas as pd

def Find_DayOfTheWeek(given_date):
    #given_date='2018-12-12'
    pd.to_datetime(given_date)
    #Timestamp('2018-12-12 00:00:00')
    orig_date=pd.to_datetime(given_date)
    orig_date.weekday()
    Day_Nm=calendar.day_name[orig_date.weekday()]
    print("Day of the Input_Date :", Day_Nm)


if __name__ == "__main__":
    Input_Date = str(sys.argv[1])
    Find_DayOfTheWeek(Input_Date)
	

