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
        msg = company + " " + str(result) + '\n'
        l = {}
        l["msg"] = msg
        l["minDate"] = date_min
        l["alwaysUp"] = always_up
        l["name"] = company
        yield  None, l
    
    def reducer_always_up(self, date_min, companies):
        alwaysUp = []
        lowestDays = {}
        blackDay = ''
        blackDayValue = 0

        for company in companies:
            stdout.write(company["msg"])
            if company["alwaysUp"]:
                alwaysUp.append(company["name"])
            
            date_min = company["minDate"]
            if date_min not in lowestDays:
                lowestDays[date_min] = 1
            else:
                lowestDays[date_min] += 1
            if lowestDays[date_min] > blackDayValue:
                blackDay = date_min
                blackDayValue = lowestDays[date_min]
        
        stdout.write("Companies always up: " + str(alwaysUp) + '\n')
        stdout.write("Black day: " + blackDay + ". Number of companies with lowest value: " + str(blackDayValue) + '\n')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_company,
                   reducer=self.reducer_company),
            MRStep(reducer=self.reducer_always_up)
        ]

if __name__ == '__main__':
    MRCompaniesValue.run()