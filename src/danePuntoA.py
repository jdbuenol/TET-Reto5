from mrjob.job import MRJob

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

if __name__ == '__main__':
    MRAverageSE.run()