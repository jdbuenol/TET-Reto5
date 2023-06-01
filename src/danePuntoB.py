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

if __name__ == '__main__':
    MRAverageEmployee.run()