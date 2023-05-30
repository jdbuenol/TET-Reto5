from mrjob.job import MRJob

alwaysUp = []
lowestDayValue = {}
blackDay = ""
blackDayValue = 0

class MRCompaniesValue(MRJob):

    def mapper(self, _, line):
        company, price, date = line.split(',')
        value = {}
        value["price"] = price
        value["date"] = date
        yield company, value

    def reducer(self, company, gen):
        global blackDayValue
        global blackDay

        values = list(gen)
        minvalue = values[0]["price"]
        date_min = values[0]["date"]
        maxvalue = values[0]["price"]
        date_max = values[0]["date"]
        always_up = True
        for x in values:
            if x["price"] < minvalue:
                minvalue = x["price"]
                date_min = x["date"]
                always_up = False
            elif maxvalue < x["price"]:
                maxvalue = x["price"]
                date_max = x["date"]
    
        if always_up:
            alwaysUp.append(company)

        if date_min not in lowestDayValue:
            lowestDayValue[date_min] = 1
        else:
            lowestDayValue[date_min] += 1
        if lowestDayValue[date_min] > blackDayValue:
            blackDayValue = lowestDayValue[date_min]
            blackDay = date_min

        result = {}
        result["minValue"] = minvalue
        result["minValueDate"] = date_min
        result["maxValue"] = maxvalue
        result["maxValueDate"] = date_max
        yield company, result

if __name__ == '__main__':
    MRCompaniesValue.run()
    print("Companies that always go up: " + str(alwaysUp))
    print("Black Day: " + blackDay + ". Companies with lowest value: " + str(blackDayValue))