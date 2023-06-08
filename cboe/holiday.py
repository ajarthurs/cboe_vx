from pandas.tseries.holiday import (
        AbstractHolidayCalendar,
        Holiday,
        sunday_to_monday,
        nearest_workday,
        USMartinLutherKingJr,
        USPresidentsDay,
        GoodFriday,
        USMemorialDay,
        USLaborDay,
        USThanksgivingDay,
        )

class USMarketHolidayCalendar(AbstractHolidayCalendar):
    """
    US market holiday calendar based on rules specified by:
    https://www.nyse.com/markets/hours-calendars
    """
    rules = [
        Holiday('New Years Day', month=1, day=1, observance=sunday_to_monday),
        USMartinLutherKingJr,
        USPresidentsDay,
        GoodFriday,
        USMemorialDay,
        Holiday('Juneteenth National Independence Day', month=6, day=19, observance=nearest_workday),
        Holiday('Independence Day', month=7, day=4, observance=nearest_workday),
        USLaborDay,
        USThanksgivingDay,
        Holiday('Christmas', month=12, day=25, observance=nearest_workday)
    ]
#END: USMarketHolidayCalendar
