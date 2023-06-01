from mrjob.job import MRJob

class MRAverageEmployee(MRJob):

    def mapper(self, _, line):
        idemp, sector,salary,year = line.split(',')
        yield idemp, float(salary)

    def reducer(self, idemp, values):
        salaries = 0
        counter = 0
        for salary in values:
            salaries += salary
            counter += 1
        l = salaries / counter
        yield "average employee #" + idemp, l

class MRAverageSE(MRJob):
    def mapper(self, _, line):
        idemp, sector, salary, year = line.split(',')
        yield sector, float(salary)
    
    def reducer(self, SE, values):
        salaries = 0
        counter = 0
        for salary in values:
            salaries += salary
            counter += 1
        l = salaries / counter
        yield "average Sector Economico #" + SE, l

class MRSEPerEmployee(MRJob):
    def mapper(self, _, line):
        idemp, sector, salary, year = line.split(',')
        yield idemp, sector
    
    def reducer(self, idemp, sectors):
        recordedSectors = []
        for sector in sectors:
            if not sector in recordedSectors:
                recordedSectors.append(sector)
        yield "Number of SE of employee #" + idemp, len(recordedSectors)
