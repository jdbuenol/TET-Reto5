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
        yield date_min, result
    
    def reducer_always_up(self, date_min, companies):
        alwaysUp = []

        for company in companies:
            if company["alwaysUp"]:
                alwaysUp.append(company["name"])
        
        stdout.write("companies always up:" + alwaysUp)
        yield None, None

    def steps(self):
        return [
            MRStep(mapper=self.mapper_company,
                   reducer=self.reducer_company),
            MRStep(reducer=self.reducer_black_day)
        ]

if __name__ == '__main__':
    MRCompaniesValue.run()