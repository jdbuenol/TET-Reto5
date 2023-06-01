from mrjob.job import MRJob
from mrjob.step import MRStep
from sys import stdout

class MRMostViewsDay(MRJob):
    
    def mapper_get_date(self, _, line):
        user, movie, rating, genre, date = line.split(',')
        yield date, 1
    
    def combiner_get_date(self, date, value):
        yield date, sum(value)
    
    def reducer_get_date(self, date, value):
        yield None, (sum(value), date)
    
    def reducer_find_most_views_day(self, _, gen):
        data = list(gen)
        most_views_pair = max(data)
        least_views_pair = min(data)
        l = {}
        l["most_views"] = most_views_pair[0]
        l["most_views_day"] = most_views_pair[1]
        l["least_views"] = least_views_pair[0]
        l["least_views_day"] = least_views_pair[1]
        stdout.write(str(l))
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_date,
                   combiner=self.combiner_get_date,
                   reducer=self.reducer_get_date),
            MRStep(reducer=self.reducer_find_most_views_day)
        ]

if __name__ == '__main__':
    MRMostViewsDay.run()