from mrjob.job import MRJob, MRStep
from sys import stdout

class MRCompaniesValue(MRJob):

    def mapper_company(self, _, line):
        company, price, date = line.split(',')
        value = {}
        value["price"] = price
        value["date"] = date
        yield company, value

    def reducer_company(self, company, gen):
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

        result = {}
        result["minValue"] = minvalue
        result["minValueDate"] = date_min
        result["maxValue"] = maxvalue
        result["maxValueDate"] = date_max
        msg = str(company) + " " + str(result) + '\n'
        stdout.write(msg)
        result["alwaysUp"] = always_up
        result["name"] = company
        yield None, result
    
    def reducer_black_day(self, _, companies):
        alwaysUp = []
        lowestDayValue = {}
        blackDay = ""
        blackDayValue = 0

        for company in companies:
            if company["alwaysUp"]:
                alwaysUp.append(company["name"])
            date_min = company["minValueDate"]
            if date_min not in lowestDayValue:
                lowestDayValue[date_min] = 1
            else:
                lowestDayValue[date_min] += 1
            if lowestDayValue[date_min] > blackDayValue:
                blackDayValue = lowestDayValue[date_min]
                blackDay = date_min
        
        l = {}
        l["alwaysUp"] = str(alwaysUp)
        l["BlackDay"] = str(blackDay)
        l["CompaniesWithLowestValueInBlackDay"] = str(blackDayValue)
        yield None, l

    def steps(self):
        return [
            MRStep(mapper=self.mapper_company,
                   reducer=self.reducer_company),
            MRStep(reducer=self.reducer_black_day)
        ]

if __name__ == '__main__':
    MRCompaniesValue.run()