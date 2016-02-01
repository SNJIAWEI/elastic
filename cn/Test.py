import datetime

TIME_FMT_YMD = '%Y-%m-%d'
# today = datetime.date.today().strftime(TIME_FMT_YMD)

yestoday = (datetime.date.today() - datetime.timedelta(days=1)).strftime(TIME_FMT_YMD)

# print(today)
print(yestoday)