from pandas.tseries.holiday import *

class USMarketHolidayCalendar(AbstractHolidayCalendar):
    """
    US market holiday calendar based on rules specified by:
    https://www.nyse.com/markets/hours-calendars
    """
    rules = [
        Holiday('New Years Day', month=1, day=1, observance=nearest_workday),
        USMartinLutherKingJr,
        USPresidentsDay,
        GoodFriday,
        USMemorialDay,
        Holiday('July 4th', month=7, day=4, observance=nearest_workday),
        USLaborDay,
        USThanksgivingDay,
        Holiday('Christmas', month=12, day=25, observance=nearest_workday)
    ]
#END: USMarketHolidayCalendar
